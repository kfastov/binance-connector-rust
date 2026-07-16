use async_trait::async_trait;
use flate2::read::ZlibDecoder;
use futures::{SinkExt, StreamExt, future::try_join_all, stream::FuturesUnordered};
use http::header::USER_AGENT;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    io::Read,
    marker::PhantomData,
    mem::take,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    select, spawn,
    sync::{
        Mutex, Notify,
        mpsc::{Receiver, Sender, UnboundedSender, channel, unbounded_channel},
        oneshot, watch,
    },
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use tokio_util::time::DelayQueue;
use tracing::{debug, error, info, warn};

#[cfg(feature = "derivatives_trading_usds_futures")]
use super::models::RoutedStreamTarget;
use super::{
    config::{
        AgentConnector, ConfigurationWebsocketApi, ConfigurationWebsocketStreams, RawFrameContext,
        RawFrameKind, RawFrameObserver, StreamSubscriptionObserver, WebsocketLifecycleContext,
        WebsocketLifecycleEvent,
    },
    errors::{StreamSubscriptionError, WebsocketConnectionFailureReason, WebsocketError},
    models::{
        StreamId, StreamSubscriptionAck, StreamSubscriptionContext, StreamSubscriptionEvent,
        StreamSubscriptionOutcome, StreamSubscriptionScope, WebsocketApiResponse, WebsocketEvent,
        WebsocketMode,
    },
    utils::{
        build_websocket_api_message, normalize_stream_id, random_integer, random_string,
        validate_time_unit,
    },
};

pub type WebSocketClient = WebSocketStream<MaybeTlsStream<TcpStream>>;

const MAX_CONN_DURATION: Duration = Duration::from_secs(23 * 60 * 60);
const STREAM_SUBSCRIPTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);

fn redacted_websocket_url(raw: &str) -> String {
    let Ok(parsed) = url::Url::parse(raw) else {
        return "<invalid websocket URL>".to_string();
    };
    let Some(host) = parsed.host_str() else {
        return format!("{}://<redacted>", parsed.scheme());
    };
    let host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let authority = parsed
        .port()
        .map_or_else(|| host.clone(), |port| format!("{host}:{port}"));
    let safe_scope = parsed
        .path_segments()
        .and_then(|mut segments| segments.find(|segment| !segment.is_empty()))
        .filter(|scope| {
            matches!(
                *scope,
                "market" | "public" | "private" | "stream" | "ws" | "ws-api"
            )
        });
    match safe_scope {
        Some(scope) => format!("{}://{authority}/{scope}/<redacted>", parsed.scheme()),
        None => format!("{}://{authority}/<redacted>", parsed.scheme()),
    }
}

fn stream_subscription_scope(scope: Option<&str>) -> StreamSubscriptionScope {
    match scope {
        Some("market") => StreamSubscriptionScope::Market,
        Some("public") => StreamSubscriptionScope::Public,
        Some("private") => StreamSubscriptionScope::Private,
        Some("stream") => StreamSubscriptionScope::Stream,
        Some("ws") => StreamSubscriptionScope::Ws,
        Some("ws-api") => StreamSubscriptionScope::WsApi,
        Some(_) => StreamSubscriptionScope::Other,
        None => StreamSubscriptionScope::Default,
    }
}

fn observer_path_scope(scope: Option<&str>) -> Option<&'static str> {
    match stream_subscription_scope(scope) {
        StreamSubscriptionScope::Default => None,
        StreamSubscriptionScope::Market => Some("market"),
        StreamSubscriptionScope::Public => Some("public"),
        StreamSubscriptionScope::Private => Some("private"),
        StreamSubscriptionScope::Stream => Some("stream"),
        StreamSubscriptionScope::Ws => Some("ws"),
        StreamSubscriptionScope::WsApi => Some("ws-api"),
        StreamSubscriptionScope::Other => Some("other"),
    }
}

fn redacted_path_scope(scope: Option<&str>) -> &'static str {
    match stream_subscription_scope(scope) {
        StreamSubscriptionScope::Default => "default",
        StreamSubscriptionScope::Market => "market",
        StreamSubscriptionScope::Public => "public",
        StreamSubscriptionScope::Private => "private",
        StreamSubscriptionScope::Stream => "stream",
        StreamSubscriptionScope::Ws => "ws",
        StreamSubscriptionScope::WsApi => "ws-api",
        StreamSubscriptionScope::Other => "other",
    }
}

pub struct Subscription {
    handle: JoinHandle<()>,
}

impl Subscription {
    /// Cancels the ongoing WebSocket event subscription and stops the event processing task.
    ///
    /// This method aborts the background task responsible for receiving and processing
    /// WebSocket events, effectively unsubscribing from further event notifications.
    ///
    /// # Examples
    ///
    ///
    /// let emitter = `WebsocketEventEmitter::new()`;
    /// let subscription = emitter.subscribe(|event| {
    ///     // Handle WebSocket event
    /// });
    /// `subscription.unsubscribe()`; // Stop receiving events
    ///
    pub fn unsubscribe(self) {
        self.handle.abort();
    }
}

#[derive(Clone)]
pub enum WebsocketBase {
    WebsocketApi(Arc<WebsocketApi>),
    WebsocketStreams(Arc<WebsocketStreams>),
}

pub struct WebsocketEventEmitter {
    subscribers: Arc<std::sync::Mutex<Vec<UnboundedSender<WebsocketEvent>>>>,
}

#[derive(Clone)]
struct StreamSubscriptionEventEmitter {
    subscribers: Arc<std::sync::Mutex<Vec<UnboundedSender<StreamSubscriptionEvent>>>>,
    synchronous: Option<StreamSubscriptionObserver>,
}

impl StreamSubscriptionEventEmitter {
    fn new(synchronous: Option<StreamSubscriptionObserver>) -> Self {
        Self {
            subscribers: Arc::new(std::sync::Mutex::new(Vec::new())),
            synchronous,
        }
    }

    fn subscribe<F>(&self, mut callback: F) -> Subscription
    where
        F: FnMut(StreamSubscriptionEvent) + Send + 'static,
    {
        let (tx, mut rx) = unbounded_channel();
        let mut guard = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push(tx);
        drop(guard);

        let handle = spawn(async move {
            while let Some(event) = rx.recv().await {
                callback(event);
            }
        });
        Subscription { handle }
    }

    fn emit(&self, event: &StreamSubscriptionEvent) {
        if let Some(observer) = &self.synchronous {
            observer.observe(event);
        }
        let mut guard = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.retain(|tx| tx.send(event.clone()).is_ok());
    }
}

struct StreamSubscriptionLifecycle {
    context: std::sync::Mutex<Option<StreamSubscriptionContext>>,
    dispatched: AtomicBool,
    terminal_emitted: AtomicBool,
}

impl StreamSubscriptionLifecycle {
    fn new() -> Self {
        Self {
            context: std::sync::Mutex::new(None),
            dispatched: AtomicBool::new(false),
            terminal_emitted: AtomicBool::new(false),
        }
    }

    fn set_context(&self, context: StreamSubscriptionContext) {
        let mut stored = match self.context.lock() {
            Ok(stored) => stored,
            Err(poisoned) => poisoned.into_inner(),
        };
        *stored = Some(context);
    }

    fn context(&self) -> Option<StreamSubscriptionContext> {
        let stored = match self.context.lock() {
            Ok(stored) => stored,
            Err(poisoned) => poisoned.into_inner(),
        };
        stored.clone()
    }

    fn emit_dispatched(
        &self,
        emitter: &StreamSubscriptionEventEmitter,
        context: &StreamSubscriptionContext,
    ) {
        self.dispatched.store(true, Ordering::Release);
        emitter.emit(&StreamSubscriptionEvent {
            context: context.clone(),
            outcome: StreamSubscriptionOutcome::Dispatched,
        });
    }

    fn emit_terminal(
        &self,
        emitter: &StreamSubscriptionEventEmitter,
        context: StreamSubscriptionContext,
        outcome: StreamSubscriptionOutcome,
    ) -> bool {
        if !self.try_claim_terminal() {
            return false;
        }
        Self::emit_claimed(emitter, context, outcome);
        true
    }

    fn try_claim_terminal(&self) -> bool {
        self.terminal_emitted
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn emit_claimed(
        emitter: &StreamSubscriptionEventEmitter,
        context: StreamSubscriptionContext,
        outcome: StreamSubscriptionOutcome,
    ) {
        emitter.emit(&StreamSubscriptionEvent { context, outcome });
    }

    fn claim_cancelled_if_dispatched(&self) -> Option<StreamSubscriptionContext> {
        if !self.dispatched.load(Ordering::Acquire) {
            return None;
        }
        let context = self.context()?;
        if self.try_claim_terminal() {
            Some(context)
        } else {
            None
        }
    }
}

impl Default for WebsocketEventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

impl WebsocketEventEmitter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Subscribes to WebSocket events and returns a `Subscription` that allows event processing.
    ///
    /// This method creates an unbounded channel for receiving WebSocket events and
    /// spawns an asynchronous task to process these events using the provided callback function.
    ///
    /// # Arguments
    ///
    /// * `callback` - A mutable function that will be called for each received WebSocket event.
    ///   The callback must be thread-safe and have a static lifetime.
    ///
    /// # Returns
    ///
    /// A `Subscription` that can be used to unsubscribe and stop event processing.
    ///
    /// # Examples
    ///
    ///
    /// let emitter = `WebsocketEventEmitter::new()`;
    /// let subscription = emitter.subscribe(|event| {
    ///     // Handle WebSocket event
    ///     println!("Received event: {:?}", event);
    /// });
    ///
    /// // Later, when no longer needed
    /// `subscription.unsubscribe()`;
    ///
    pub fn subscribe<F>(&self, mut callback: F) -> Subscription
    where
        F: FnMut(WebsocketEvent) + Send + 'static,
    {
        let (tx, mut rx) = unbounded_channel();
        let mut guard = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push(tx);
        drop(guard);

        let handle = spawn(async move {
            while let Some(event) = rx.recv().await {
                callback(event);
            }
        });
        Subscription { handle }
    }

    /// Emits a WebSocket event to all registered subscribers.
    ///
    /// This method sends the given event to all active subscribers. If a subscriber
    /// has been dropped without unsubscribing, a warning is logged and the subscriber
    /// is removed from the list.
    ///
    /// # Arguments
    ///
    /// * `event` - The WebSocket event to be emitted to all subscribers.
    pub fn emit(&self, event: &WebsocketEvent) {
        let mut guard = match self.subscribers.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };

        guard.retain(|tx| {
            if tx.send(event.clone()).is_ok() {
                true
            } else {
                warn!("subscriber dropped without unsubscribing");
                false
            }
        });
    }
}

/// A trait defining the lifecycle and behavior of a WebSocket connection.
///
/// This trait provides methods for handling WebSocket connection events,
/// including connection opening, message handling, and reconnection URL retrieval.
///
/// # Methods
///
/// * `on_open`: Called when a WebSocket connection is established
/// * `on_message`: Called when a message is received over the WebSocket
/// * `get_reconnect_url`: Determines the URL to use for reconnecting
///
/// # Thread Safety
///
/// Implementors must be safely shareable across threads, as indicated by the `Send + Sync + 'static` bounds.
#[async_trait]
pub trait WebsocketHandler: Send + Sync + 'static {
    async fn on_open(&self, url: String, connection: Arc<WebsocketConnection>);
    async fn on_message(&self, data: String, connection: Arc<WebsocketConnection>);
    /// Context-aware message hook. The default preserves compatibility with
    /// existing handlers; stream handlers override it when a response must be
    /// correlated to one physical session generation.
    async fn on_message_with_session(
        &self,
        data: String,
        connection: Arc<WebsocketConnection>,
        _session_generation: u64,
    ) {
        self.on_message(data, connection).await;
    }
    async fn get_reconnect_url(
        &self,
        default_url: String,
        connection: Arc<WebsocketConnection>,
    ) -> String;
}

pub struct PendingRequest {
    pub completion: oneshot::Sender<Result<Value, WebsocketError>>,
}

struct PendingStreamSubscription {
    token: u64,
    context: StreamSubscriptionContext,
    #[cfg(feature = "derivatives_trading_usds_futures")]
    ownership: PendingStreamSubscriptionOwnership,
    stream_keys: Vec<String>,
    was_confirmed: bool,
    unconfirmed_reservation_token: Option<u64>,
    lifecycle: Arc<StreamSubscriptionLifecycle>,
    completion: oneshot::Sender<Result<StreamSubscriptionAck, StreamSubscriptionError>>,
}

impl PendingStreamSubscription {
    fn receiver_cancellation_wins(&self, receiver_closed: bool) -> bool {
        #[cfg(feature = "derivatives_trading_usds_futures")]
        let caller_managed = self.ownership == PendingStreamSubscriptionOwnership::CallerManaged;
        #[cfg(not(feature = "derivatives_trading_usds_futures"))]
        let caller_managed = false;

        receiver_closed && (!self.was_confirmed || caller_managed)
    }
}

#[cfg(feature = "derivatives_trading_usds_futures")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingStreamSubscriptionOwnership {
    SdkManaged,
    CallerManaged,
}

#[derive(Clone)]
pub struct WebsocketSessionLogonReq {
    pub method: String,
    pub payload: BTreeMap<String, Value>,
    pub options: WebsocketMessageSendOptions,
}

pub struct WebsocketConnectionState {
    pub reconnection_pending: bool,
    pub renewal_pending: bool,
    pub close_initiated: bool,
    pub pending_requests: HashMap<String, PendingRequest>,
    pending_stream_subscriptions: HashMap<u32, PendingStreamSubscription>,
    stream_request_id_generations: HashMap<u32, u64>,
    pub pending_subscriptions: VecDeque<String>,
    pub stream_callbacks: HashMap<String, Vec<Arc<dyn Fn(&Value) + Send + Sync + 'static>>>,
    confirmed_stream_keys: BTreeSet<String>,
    unconfirmed_stream_reservations: HashMap<String, u64>,
    pub is_session_logged_on: bool,
    pub session_logon_req: Option<WebsocketSessionLogonReq>,
    pub url_path: Option<String>,
    pub handler: Option<Arc<dyn WebsocketHandler>>,
    pub ws_write_tx: Option<UnboundedSender<Message>>,
    writer_session_generation: Option<u64>,
}

impl Default for WebsocketConnectionState {
    fn default() -> Self {
        Self::new()
    }
}

impl WebsocketConnectionState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            reconnection_pending: false,
            renewal_pending: false,
            close_initiated: false,
            pending_requests: HashMap::new(),
            pending_stream_subscriptions: HashMap::new(),
            stream_request_id_generations: HashMap::new(),
            pending_subscriptions: VecDeque::new(),
            stream_callbacks: HashMap::new(),
            confirmed_stream_keys: BTreeSet::new(),
            unconfirmed_stream_reservations: HashMap::new(),
            is_session_logged_on: false,
            session_logon_req: None,
            url_path: None,
            handler: None,
            ws_write_tx: None,
            writer_session_generation: None,
        }
    }

    fn remove_pending_stream_subscription(
        &mut self,
        request_id: u32,
        claim_terminal: bool,
    ) -> Option<(PendingStreamSubscription, bool)> {
        let terminal_claimed = claim_terminal
            && self
                .pending_stream_subscriptions
                .get(&request_id)
                .is_some_and(|pending| pending.lifecycle.try_claim_terminal());
        self.pending_stream_subscriptions
            .remove(&request_id)
            .map(|pending| (pending, terminal_claimed))
    }
}

pub struct WebsocketConnection {
    pub id: String,
    session_generation: AtomicU64,
    init_lock: Mutex<()>,
    pub drain_notify: Notify,
    pub state: Mutex<WebsocketConnectionState>,
}

impl WebsocketConnection {
    pub fn new(id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            session_generation: AtomicU64::new(0),
            init_lock: Mutex::new(()),
            drain_notify: Notify::new(),
            state: Mutex::new(WebsocketConnectionState::new()),
        })
    }

    pub async fn set_handler(&self, handler: Arc<dyn WebsocketHandler>) {
        let mut conn_state = self.state.lock().await;
        conn_state.handler = Some(handler);
    }
}

struct ReconnectEntry {
    connection_id: String,
    session_generation: u64,
    path_scope: Option<String>,
    url: String,
    is_renewal: bool,
}

struct RenewalEntry {
    connection_id: String,
    session_generation: u64,
    path_scope: Option<String>,
    url: String,
}

pub struct WebsocketCommon {
    pub events: WebsocketEventEmitter,
    mode: WebsocketMode,
    round_robin_index: AtomicUsize,
    connection_pool: Vec<Arc<WebsocketConnection>>,
    reconnect_tx: Sender<ReconnectEntry>,
    renewal_tx: Sender<RenewalEntry>,
    reconnect_delay: usize,
    agent: Option<AgentConnector>,
    user_agent: Option<String>,
    raw_frame_observer: Option<RawFrameObserver>,
    /// Whether this SDK runtime may replace physical sessions on its own.
    /// Exact-scope USD-M clients switch this off before their first handshake
    /// so the connector can own backoff, jitter, silence recovery and replay.
    automatic_session_replacement: AtomicBool,
    #[cfg(test)]
    renewal_enqueue_count: AtomicU64,
    terminal_shutdown: AtomicBool,
    terminal_shutdown_tx: watch::Sender<bool>,
    background_tasks: StdMutex<Vec<JoinHandle<()>>>,
}

impl WebsocketCommon {
    #[must_use]
    pub fn new(
        initial_pool: Vec<Arc<WebsocketConnection>>,
        mode: WebsocketMode,
        reconnect_delay: usize,
        agent: Option<AgentConnector>,
        user_agent: Option<String>,
    ) -> Arc<Self> {
        Self::new_with_raw_frame_observer(
            initial_pool,
            mode,
            reconnect_delay,
            agent,
            user_agent,
            None,
        )
    }

    /// Creates a WebSocket common runtime with a synchronous pre-decode
    /// observer for Text/Binary payloads.
    #[must_use]
    pub fn new_with_raw_frame_observer(
        mut initial_pool: Vec<Arc<WebsocketConnection>>,
        mode: WebsocketMode,
        reconnect_delay: usize,
        agent: Option<AgentConnector>,
        user_agent: Option<String>,
        raw_frame_observer: Option<RawFrameObserver>,
    ) -> Arc<Self> {
        if initial_pool.is_empty() {
            for _ in 0..mode.pool_size() {
                let id = random_string();
                initial_pool.push(WebsocketConnection::new(id));
            }
        }

        let (reconnect_tx, reconnect_rx) = channel::<ReconnectEntry>(mode.pool_size());
        let (renewal_tx, renewal_rx) = channel::<RenewalEntry>(mode.pool_size());
        let (terminal_shutdown_tx, _) = watch::channel(false);

        let common = Arc::new(Self {
            events: WebsocketEventEmitter::new(),
            mode,
            round_robin_index: AtomicUsize::new(0),
            connection_pool: initial_pool,
            reconnect_tx,
            renewal_tx,
            reconnect_delay,
            agent,
            user_agent,
            raw_frame_observer,
            automatic_session_replacement: AtomicBool::new(true),
            #[cfg(test)]
            renewal_enqueue_count: AtomicU64::new(0),
            terminal_shutdown: AtomicBool::new(false),
            terminal_shutdown_tx,
            background_tasks: StdMutex::new(Vec::new()),
        });

        let terminal_shutdown_rx = common.terminal_shutdown_tx.subscribe();
        let reconnect_task = Self::spawn_reconnect_loop(
            Arc::clone(&common),
            reconnect_rx,
            terminal_shutdown_rx.clone(),
        );
        let renewal_task = Self::spawn_renewal_loop(&common, renewal_rx, terminal_shutdown_rx);
        common
            .background_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend([reconnect_task, renewal_task]);

        common
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    pub(crate) fn use_caller_managed_session_replacement(&self) {
        debug_assert!(
            self.connection_pool
                .iter()
                .all(|connection| connection.session_generation.load(Ordering::Acquire) == 0),
            "session-replacement ownership must be selected before the first handshake"
        );
        self.automatic_session_replacement
            .store(false, Ordering::Release);
    }

    pub(crate) fn sdk_manages_session_replacement(&self) -> bool {
        self.automatic_session_replacement.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn renewal_enqueue_count(&self) -> u64 {
        self.renewal_enqueue_count.load(Ordering::Acquire)
    }

    fn register_background_task(&self, task: JoinHandle<()>) {
        let mut tasks = self
            .background_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tasks.retain(|existing| !existing.is_finished());
        tasks.push(task);
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    fn begin_terminal_shutdown(&self) {
        if !self.terminal_shutdown.swap(true, Ordering::AcqRel) {
            self.terminal_shutdown_tx.send_replace(true);
        }
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    async fn finish_terminal_shutdown(&self) {
        self.begin_terminal_shutdown();

        // Serialize with every physical initializer. Once this guard is
        // observed after the terminal flag, no initializer can publish a new
        // writer or actor behind the shutdown boundary.
        for connection in &self.connection_pool {
            let _init_guard = connection.init_lock.lock().await;
            let writer = {
                let mut state = connection.state.lock().await;
                state.close_initiated = true;
                state.reconnection_pending = false;
                state.renewal_pending = false;
                state.is_session_logged_on = false;
                state.handler = None;
                state.writer_session_generation = None;
                state.ws_write_tx.take()
            };
            if let Some(writer) = writer {
                let _ = writer.send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "".into(),
                })));
            }
        }

        let tasks = {
            let mut tasks = self
                .background_tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            take(&mut *tasks)
        };
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn registered_background_task_count(&self) -> usize {
        self.background_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    pub(crate) async fn installed_handler_count(&self) -> usize {
        let mut count = 0;
        for connection in &self.connection_pool {
            if connection.state.lock().await.handler.is_some() {
                count += 1;
            }
        }
        count
    }

    #[cfg(test)]
    pub(crate) async fn installed_writer_count(&self) -> usize {
        let mut count = 0;
        for connection in &self.connection_pool {
            if connection.state.lock().await.ws_write_tx.is_some() {
                count += 1;
            }
        }
        count
    }

    #[cfg(test)]
    pub(crate) async fn pending_stream_subscription_count(&self) -> usize {
        let mut count = 0;
        for connection in &self.connection_pool {
            count += connection
                .state
                .lock()
                .await
                .pending_stream_subscriptions
                .len();
        }
        count
    }

    #[cfg(test)]
    pub(crate) fn terminal_shutdown_started(&self) -> bool {
        self.terminal_shutdown.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) async fn lock_first_connection_state(
        &self,
    ) -> tokio::sync::MutexGuard<'_, WebsocketConnectionState> {
        self.connection_pool
            .first()
            .expect("test runtime must own one connection")
            .state
            .lock()
            .await
    }

    /// Spawns an asynchronous loop to handle websocket reconnection attempts
    ///
    /// This method manages reconnection logic for websocket connections, including:
    /// - Scheduling reconnects with a configurable delay
    /// - Finding the appropriate connection in the connection pool
    /// - Attempting to reinitialize the connection
    /// - Logging reconnection failures or warnings
    ///
    /// # Arguments
    /// * `common` - A shared reference to the `WebsocketCommon` instance
    /// * `reconnect_rx` - A receiver channel for reconnection entries
    ///
    /// # Behavior
    /// - Waits for reconnection entries from the channel
    /// - Applies a configurable delay before attempting reconnection
    /// - Attempts to reinitialize the connection with the provided URL
    /// - Handles and logs any reconnection errors
    fn spawn_reconnect_loop(
        common: Arc<Self>,
        mut reconnect_rx: Receiver<ReconnectEntry>,
        mut terminal_shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        spawn(async move {
            loop {
                let entry = select! {
                    biased;
                    changed = terminal_shutdown_rx.changed() => {
                        if changed.is_err() || *terminal_shutdown_rx.borrow() {
                            break;
                        }
                        continue;
                    }
                    entry = reconnect_rx.recv() => {
                        let Some(entry) = entry else { break };
                        entry
                    }
                };
                if !common.sdk_manages_session_replacement() {
                    debug!(
                        "Discarding SDK reconnect for caller-managed connection {} generation {}",
                        entry.connection_id, entry.session_generation
                    );
                    continue;
                }
                info!("Scheduling reconnect for id {}", entry.connection_id);

                if let Some(conn_arc) = common
                    .connection_pool
                    .iter()
                    .find(|c| c.id == entry.connection_id)
                    .cloned()
                {
                    if conn_arc.session_generation.load(Ordering::Acquire)
                        != entry.session_generation
                    {
                        debug!(
                            "Discarding stale reconnect for connection {} generation {}",
                            entry.connection_id, entry.session_generation
                        );
                        continue;
                    }
                    common.observe_lifecycle(
                        &entry.connection_id,
                        entry.session_generation,
                        entry.path_scope.as_deref(),
                        WebsocketLifecycleEvent::ReconnectScheduled {
                            is_renewal: entry.is_renewal,
                        },
                    );
                    if !entry.is_renewal {
                        select! {
                            biased;
                            _ = terminal_shutdown_rx.changed() => break,
                            () = sleep(Duration::from_millis(common.reconnect_delay as u64)) => {}
                        }
                    }
                    if common.terminal_shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    if conn_arc.session_generation.load(Ordering::Acquire)
                        != entry.session_generation
                    {
                        debug!(
                            "Skipping stale reconnect attempt for connection {} generation {}",
                            entry.connection_id, entry.session_generation
                        );
                        continue;
                    }
                    let reconnect_url = common
                        .get_reconnect_url(&entry.url, Arc::clone(&conn_arc))
                        .await;
                    let common_clone = Arc::clone(&common);
                    let reconnect_result = select! {
                        biased;
                        _ = terminal_shutdown_rx.changed() => break,
                        result = common_clone.init_connect(
                            &reconnect_url,
                            entry.is_renewal,
                            Some(conn_arc.clone()),
                            Some(entry.session_generation),
                        )
                        => result,
                    };
                    if reconnect_result.is_err() {
                        error!(
                            "Reconnect failed for {} → {}",
                            entry.connection_id,
                            redacted_websocket_url(&reconnect_url)
                        );
                    }

                    select! {
                        biased;
                        _ = terminal_shutdown_rx.changed() => break,
                        () = sleep(Duration::from_secs(1)) => {}
                    }
                } else {
                    warn!("No connection {} found for reconnect", entry.connection_id);
                }
            }
        })
    }

    /// Spawns an asynchronous loop to manage connection renewals
    ///
    /// This method handles the periodic renewal of websocket connections by:
    /// - Maintaining a delay queue for connection expiration
    /// - Receiving renewal requests for specific connections
    /// - Triggering reconnection when a connection reaches its maximum duration
    /// - Attempting to find and renew connections in the connection pool
    ///
    /// # Behavior
    /// - Listens for renewal requests on a channel
    /// - Tracks connection expiration using a delay queue
    /// - Initiates reconnection process when a connection expires
    /// - Handles and logs any renewal failures
    fn spawn_renewal_loop(
        common: &Arc<Self>,
        renewal_rx: Receiver<RenewalEntry>,
        mut terminal_shutdown_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let common = Arc::clone(common);
        spawn(async move {
            let mut dq = DelayQueue::new();
            let mut renewal_rx = renewal_rx;

            loop {
                select! {
                    biased;
                    changed = terminal_shutdown_rx.changed() => {
                        if changed.is_err() || *terminal_shutdown_rx.borrow() {
                            break;
                        }
                    }
                    Some(entry) = renewal_rx.recv() => {
                        debug!("Scheduling renewal for {}", entry.connection_id);
                        dq.insert(entry, MAX_CONN_DURATION);
                    }

                    Some(expired) = dq.next() => {
                        let entry = expired.into_inner();

                        if let Some(conn_arc) = common
                            .connection_pool
                            .iter()
                            .find(|c| c.id == entry.connection_id)
                            .cloned()
                        {
                            if conn_arc.session_generation.load(Ordering::Acquire)
                                != entry.session_generation
                            {
                                debug!(
                                    "Discarding stale renewal for connection {} generation {}",
                                    entry.connection_id, entry.session_generation
                                );
                                continue;
                            }
                            debug!("Renewing connection {}", entry.connection_id);
                            let url = common
                                .get_reconnect_url(&entry.url, Arc::clone(&conn_arc))
                                .await;
                            if common.reconnect_tx.send(ReconnectEntry {
                                connection_id: entry.connection_id.clone(),
                                session_generation: entry.session_generation,
                                path_scope: entry.path_scope,
                                url,
                                is_renewal: true,
                            }).await.is_err() {
                                error!("Failed to enqueue renewal for {}", entry.connection_id);
                            }
                        } else {
                            warn!("No connection {} found for renewal", entry.connection_id);
                        }
                    }
                }
            }
        })
    }

    /// Checks if a WebSocket connection is ready for use.
    ///
    /// # Arguments
    ///
    /// * `connection` - The WebSocket connection to check
    /// * `allow_non_established` - If true, allows connections that are not fully established
    ///
    /// # Returns
    ///
    /// `true` if the connection is ready, `false` otherwise
    ///
    /// # Behavior
    ///
    /// A connection is considered ready if:
    /// - It has a write channel (unless `allow_non_established` is true)
    /// - No reconnection is pending
    /// - No close has been initiated
    pub async fn is_connection_ready(
        &self,
        connection: &WebsocketConnection,
        allow_non_established: bool,
    ) -> bool {
        let conn_state = connection.state.lock().await;
        (allow_non_established || conn_state.ws_write_tx.is_some())
            && !conn_state.reconnection_pending
            && !conn_state.close_initiated
    }

    /// Checks if a WebSocket connection is established.
    ///
    /// # Arguments
    ///
    /// * `connection` - Optional specific WebSocket connection to check
    ///
    /// # Returns
    ///
    /// `true` if a connection is ready and established, `false` otherwise
    ///
    /// # Behavior
    ///
    /// - If a specific connection is provided, checks only that connection
    /// - If no connection is provided, checks all connections in the pool
    /// - A connection is considered established if it is ready and not in a non-established state
    async fn is_connected(&self, connection: Option<&Arc<WebsocketConnection>>) -> bool {
        if let Some(conn_arc) = connection {
            return self.is_connection_ready(conn_arc, false).await;
        }

        for conn_arc in &self.connection_pool {
            if self.is_connection_ready(conn_arc, false).await {
                return true;
            }
        }

        false
    }

    /// Retrieves available WebSocket connections from the connection pool.
    ///
    /// # Arguments
    ///
    /// * `allow_non_established` - If `true`, includes connections that are not fully established
    /// * `url_path` - Optional URL path to filter connections
    ///
    /// # Returns
    ///
    /// A vector of `Arc<WebsocketConnection>` that are ready based on the `allow_non_established` flag
    ///
    /// # Behavior
    ///
    /// - For single connection mode, returns the first connection
    /// - For multi-connection mode, filters connections based on readiness
    /// - Uses `is_connection_ready` to determine connection availability
    async fn get_available_connections(
        &self,
        allow_non_established: bool,
        url_path: Option<&str>,
    ) -> Vec<Arc<WebsocketConnection>> {
        if matches!(self.mode, WebsocketMode::Single) && url_path.is_none() {
            return vec![Arc::clone(&self.connection_pool[0])];
        }

        let mut ready = Vec::new();
        for conn in &self.connection_pool {
            if self.is_connection_ready(conn, allow_non_established).await {
                ready.push(Arc::clone(conn));
            }
        }

        ready
    }

    /// Retrieves a WebSocket connection from the connection pool.
    ///
    /// # Arguments
    ///
    /// * `allow_non_established` - If `true`, allows selecting a connection that is not fully established
    /// * `url_path` - Optional URL path to filter connections
    ///
    /// # Returns
    ///
    /// An `Arc` to a `WebsocketConnection` from the pool, selected using round-robin strategy
    ///
    /// # Errors
    ///
    /// Returns `WebsocketError::NotConnected` if no suitable connection is available
    ///
    /// # Behavior
    ///
    /// - For single connection mode, returns the first connection
    /// - For multi-connection mode, selects a ready connection using round-robin
    /// - Filters connections based on `allow_non_established` parameter
    async fn get_connection(
        &self,
        allow_non_established: bool,
        url_path: Option<&str>,
    ) -> Result<Arc<WebsocketConnection>, WebsocketError> {
        let candidates = self
            .get_available_connections(allow_non_established, url_path)
            .await;

        let mut ready = Vec::new();
        for conn in candidates {
            if let Some(path) = url_path {
                let st = conn.state.lock().await;
                if st.url_path.as_deref() != Some(path) {
                    continue;
                }
            }
            ready.push(conn);
        }

        if ready.is_empty() {
            return Err(WebsocketError::NotConnected);
        }

        let idx = self.round_robin_index.fetch_add(1, Ordering::Relaxed) % ready.len();

        Ok(Arc::clone(&ready[idx]))
    }

    /// Gracefully closes a WebSocket connection by waiting for pending requests to complete.
    ///
    /// # Arguments
    ///
    /// * `ws_write_tx_to_close` - Sender channel for sending close message
    /// * `connection` - Shared reference to the WebSocket connection
    ///
    /// # Behavior
    ///
    /// - Waits up to 30 seconds for all pending requests to complete
    /// - Logs debug and warning messages during the closing process
    /// - Sends a normal close frame to the WebSocket
    ///
    /// # Returns
    ///
    /// `Ok(())` if connection closes successfully, otherwise a `WebsocketError`
    async fn close_connection_gracefully(
        &self,
        ws_write_tx_to_close: UnboundedSender<Message>,
        connection: Arc<WebsocketConnection>,
    ) -> Result<(), WebsocketError> {
        debug!("Waiting for pending requests to complete before disconnecting.");

        let drain = async {
            loop {
                {
                    let conn_state = connection.state.lock().await;
                    if conn_state.pending_requests.is_empty() {
                        debug!("All pending requests completed, proceeding to close.");
                        break;
                    }
                }
                connection.drain_notify.notified().await;
            }
        };

        if timeout(Duration::from_secs(30), drain).await.is_err() {
            warn!("Timeout waiting for pending requests; forcing close.");
        }

        info!("Closing WebSocket connection for {}", connection.id);
        let _ = ws_write_tx_to_close.send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        })));

        Ok(())
    }

    /// Retrieves the URL to use for reconnecting to the WebSocket.
    ///
    /// # Arguments
    ///
    /// * `default_url` - The default URL to use if no custom reconnect URL is provided
    /// * `connection` - A shared reference to the WebSocket connection
    ///
    /// # Returns
    ///
    /// The URL to use for reconnecting, either from a custom handler or the default URL
    ///
    /// # Behavior
    ///
    /// - Checks if a connection handler is available
    /// - If a handler exists, calls its `get_reconnect_url` method
    /// - Otherwise, returns the default URL
    async fn get_reconnect_url(
        &self,
        default_url: &str,
        connection: Arc<WebsocketConnection>,
    ) -> String {
        if let Some(handler) = {
            let conn_state = connection.state.lock().await;
            conn_state.handler.clone()
        } {
            return handler
                .get_reconnect_url(default_url.to_string(), Arc::clone(&connection))
                .await;
        }

        default_url.to_string()
    }

    /// Handles the WebSocket connection opening event.
    ///
    /// This method is called when a WebSocket connection is successfully established. It performs
    /// the following key actions:
    /// - Invokes the connection handler's `on_open` method if a handler is present
    /// - Logs connection information
    /// - Handles connection renewal and close scenarios
    /// - Emits a WebSocket open event
    ///
    /// # Arguments
    ///
    /// * `url` - The URL of the WebSocket server
    /// * `connection` - A shared reference to the WebSocket connection
    /// * `old_ws_writer` - Optional previous WebSocket writer for graceful connection handling
    ///
    /// # Behavior
    ///
    /// - If a connection handler exists, calls its `on_open` method
    /// - Checks for pending renewal or close states
    /// - Closes the previous connection if renewal is in progress
    /// - Emits an open event if the connection is successfully established
    async fn on_open(
        &self,
        url: String,
        connection: Arc<WebsocketConnection>,
        old_ws_writer: Option<UnboundedSender<Message>>,
    ) {
        if let Some(handler) = {
            let conn_state = connection.state.lock().await;
            conn_state.handler.clone()
        } {
            handler.on_open(url.clone(), Arc::clone(&connection)).await;
        }

        let conn_id = &connection.id;
        info!(
            "Connected to WebSocket Server with id {}: {}",
            conn_id,
            redacted_websocket_url(&url)
        );

        {
            let mut conn_state = connection.state.lock().await;

            if conn_state.renewal_pending {
                conn_state.renewal_pending = false;
                drop(conn_state);
                if let Some(tx) = old_ws_writer {
                    info!("Connection renewal in progress; closing previous connection.");
                    let _ = self
                        .close_connection_gracefully(tx, Arc::clone(&connection))
                        .await;
                }
                return;
            }

            if conn_state.close_initiated {
                drop(conn_state);
                if let Some(tx) = connection.state.lock().await.ws_write_tx.clone() {
                    info!("Close initiated; closing connection.");
                    let _ = self
                        .close_connection_gracefully(tx, Arc::clone(&connection))
                        .await;
                }
                return;
            }

            self.events.emit(&WebsocketEvent::Open);
        }
    }

    /// Handles an incoming WebSocket message
    ///
    /// # Arguments
    ///
    /// * `msg` - The received message as a string
    /// * `connection` - A shared reference to the WebSocket connection
    ///
    /// # Behavior
    ///
    /// - If a connection handler exists, awaits its `on_message` method
    /// - Emits a `WebsocketEvent::Message` event with the received message
    #[cfg(test)]
    async fn on_message(&self, msg: String, connection: Arc<WebsocketConnection>) {
        let session_generation = connection.session_generation.load(Ordering::Acquire);
        self.on_message_for_session(msg, connection, session_generation)
            .await;
    }

    async fn on_message_for_session(
        &self,
        msg: String,
        connection: Arc<WebsocketConnection>,
        session_generation: u64,
    ) {
        let handler = connection.state.lock().await.handler.clone();
        if let Some(handler) = handler {
            handler
                .on_message_with_session(msg.clone(), Arc::clone(&connection), session_generation)
                .await;
        }
        self.events.emit(&WebsocketEvent::Message(msg));
    }

    /// Observes and then dispatches a text data-frame payload. The synchronous
    /// observation establishes a strict happens-before edge with all handler
    /// decoding.
    async fn on_text_frame(
        &self,
        msg: String,
        connection: Arc<WebsocketConnection>,
        session_generation: u64,
        path_scope: Option<&str>,
    ) {
        if let Some(observer) = &self.raw_frame_observer {
            let path_scope = observer_path_scope(path_scope);
            observer.observe_frame(RawFrameContext {
                connection_id: &connection.id,
                session_generation,
                path_scope,
                kind: RawFrameKind::Text,
                payload: msg.as_bytes(),
            });
        }
        self.on_message_for_session(msg, connection, session_generation)
            .await;
    }

    /// Observes compressed bytes before decompression, then dispatches the
    /// decompressed text through the regular handler path.
    async fn on_binary_frame(
        &self,
        bin: &[u8],
        connection: Arc<WebsocketConnection>,
        session_generation: u64,
        path_scope: Option<&str>,
    ) {
        if let Some(observer) = &self.raw_frame_observer {
            let path_scope = observer_path_scope(path_scope);
            observer.observe_frame(RawFrameContext {
                connection_id: &connection.id,
                session_generation,
                path_scope,
                kind: RawFrameKind::Binary,
                payload: bin,
            });
        }

        let mut decoder = ZlibDecoder::new(bin);
        let mut decompressed = String::new();
        if let Err(err) = decoder.read_to_string(&mut decompressed) {
            error!("Binary message decompress failed: {:?}", err);
            return;
        }
        self.on_message_for_session(decompressed, connection, session_generation)
            .await;
    }

    fn observe_lifecycle(
        &self,
        connection_id: &str,
        session_generation: u64,
        path_scope: Option<&str>,
        event: WebsocketLifecycleEvent,
    ) {
        if let Some(observer) = &self.raw_frame_observer {
            let path_scope = observer_path_scope(path_scope);
            observer.observe_lifecycle(WebsocketLifecycleContext {
                connection_id,
                session_generation,
                path_scope,
                event,
            });
        }
    }

    /// Creates a WebSocket connection with optional configuration and agent
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket server URL to connect to
    /// * `agent` - Optional agent connector for configuring the connection
    /// * `user_agent` - Optional custom user agent string
    ///
    /// # Returns
    ///
    /// A `Result` containing the established WebSocket stream or a `WebsocketError`
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if:
    /// - The WebSocket handshake fails
    /// - The connection times out after 10 seconds
    ///
    /// # Behavior
    ///
    /// Attempts to establish a WebSocket connection with a configurable timeout,
    /// supporting optional TLS, custom user agent, and connection connectors
    async fn create_websocket(
        url: &str,
        agent: Option<AgentConnector>,
        user_agent: Option<String>,
    ) -> Result<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, WebsocketError> {
        let mut req = url.into_client_request().map_err(|error| {
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            WebsocketError::Handshake(format!("{reason:?}"))
        })?;

        if let Some(ua) = user_agent {
            req.headers_mut().insert(USER_AGENT, ua.parse().unwrap());
        }

        let ws_config: Option<WebSocketConfig> = None;
        let disable_nagle = false;
        let connector: Option<Connector> = agent.map(|dbg| dbg.0);

        let timeout_duration = Duration::from_secs(10);
        let handshake = connect_async_tls_with_config(req, ws_config, disable_nagle, connector);
        match timeout(timeout_duration, handshake).await {
            Ok(Ok((ws_stream, response))) => {
                debug!("WebSocket connected with status {}", response.status());
                Ok(ws_stream)
            }
            Ok(Err(e)) => {
                let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&e);
                error!("WebSocket handshake failed: {:?}", reason);
                Err(WebsocketError::Handshake(format!("{reason:?}")))
            }
            Err(_) => {
                error!(
                    "WebSocket connection timed out after {}s",
                    timeout_duration.as_secs()
                );
                Err(WebsocketError::Timeout)
            }
        }
    }

    /// Connects to a WebSocket URL for all connections in the connection pool concurrently
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket server URL to connect to
    /// * `connections` - Optional specific connections to use, otherwise uses the entire pool
    ///
    /// # Returns
    ///
    /// A `Result` indicating whether all connections were successfully established
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if any connection in the pool fails to establish
    ///
    /// # Behavior
    ///
    /// Attempts to initialize a WebSocket connection for each connection in the pool
    /// concurrently, logging successes and failures for each connection attempt
    async fn connect_pool(
        self: Arc<Self>,
        url: &str,
        connections: Option<Vec<Arc<WebsocketConnection>>>,
    ) -> Result<(), WebsocketError> {
        let pool: Vec<Arc<WebsocketConnection>> = match connections {
            Some(v) => v,
            None => self.connection_pool.clone(),
        };

        let mut tasks = FuturesUnordered::new();

        for conn in pool {
            let common = Arc::clone(&self);
            let url = url.to_owned();

            tasks.push(async move {
                match common.init_connect(&url, false, Some(conn), None).await {
                    Ok(()) => {
                        info!("Successfully connected to {}", redacted_websocket_url(&url));
                        Ok(())
                    }
                    Err(err) => {
                        error!("Failed to connect to {}", redacted_websocket_url(&url));
                        Err(err)
                    }
                }
            });
        }

        while let Some(result) = tasks.next().await {
            result?;
        }

        Ok(())
    }

    /// Initializes a WebSocket connection for a specific connection in the pool
    ///
    /// # Arguments
    ///
    /// * `url` - The WebSocket server URL to connect to
    /// * `is_renewal` - Flag indicating whether this is a connection renewal attempt
    /// * `connection` - Optional specific WebSocket connection to use, otherwise selects from the pool
    ///
    /// # Returns
    ///
    /// A `Result` indicating whether the connection was successfully established
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if the connection fails to initialize or establish
    ///
    /// # Behavior
    ///
    /// Handles connection establishment, splitting read/write streams, spawning reader/writer tasks,
    /// and managing connection state including renewal, reconnection, and error handling
    async fn init_connect(
        self: Arc<Self>,
        url: &str,
        is_renewal: bool,
        connection: Option<Arc<WebsocketConnection>>,
        expected_session_generation: Option<u64>,
    ) -> Result<(), WebsocketError> {
        if self.terminal_shutdown.load(Ordering::Acquire) {
            return Err(WebsocketError::NotConnected);
        }
        let conn = connection.unwrap_or(self.get_connection(true, None).await?);
        let _init_guard = conn.init_lock.lock().await;
        if self.terminal_shutdown.load(Ordering::Acquire) {
            return Err(WebsocketError::NotConnected);
        }
        if expected_session_generation
            .is_some_and(|expected| conn.session_generation.load(Ordering::Acquire) != expected)
        {
            debug!("Discarding stale init attempt for connection {}", conn.id);
            return Ok(());
        }

        let path_scope = {
            let mut conn_state = conn.state.lock().await;
            if conn_state.renewal_pending && is_renewal {
                info!(
                    "Renewal in progress {}→{}",
                    conn.id,
                    redacted_websocket_url(url)
                );
                return Ok(());
            }
            if conn_state.ws_write_tx.is_some() && !is_renewal && !conn_state.reconnection_pending {
                info!(
                    "Exists {}; skipping {}",
                    conn.id,
                    redacted_websocket_url(url)
                );
                return Ok(());
            }
            if is_renewal {
                conn_state.renewal_pending = true;
            }

            conn_state.is_session_logged_on = false;
            conn_state.url_path.clone()
        };

        let ws = Self::create_websocket(url, self.agent.clone(), self.user_agent.clone())
            .await
            .inspect_err(|_| {
                error!("Handshake failed {}", redacted_websocket_url(url));
            })?;
        if self.terminal_shutdown.load(Ordering::Acquire) {
            return Err(WebsocketError::NotConnected);
        }
        let session_generation = conn
            .session_generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                WebsocketError::ServerError(format!(
                    "WebSocket session generation exhausted for connection {}",
                    conn.id
                ))
            })?
            + 1;

        info!("Established {} → {}", conn.id, redacted_websocket_url(url));

        if self.sdk_manages_session_replacement() {
            let scheduled = self.renewal_tx.try_send(RenewalEntry {
                connection_id: conn.id.clone(),
                session_generation,
                path_scope: path_scope.clone(),
                url: url.to_string(),
            });
            #[cfg(test)]
            if scheduled.is_ok() {
                self.renewal_enqueue_count.fetch_add(1, Ordering::AcqRel);
            }
            if scheduled.is_err() {
                error!("Failed to schedule renewal for {}", conn.id);
            }
        }

        let (write_half, mut read_half) = ws.split();
        let (tx, mut rx) = unbounded_channel::<Message>();

        let old_writer = {
            let mut conn_state = conn.state.lock().await;
            conn_state.reconnection_pending = false;
            conn_state.writer_session_generation = Some(session_generation);
            conn_state.stream_request_id_generations.clear();
            conn_state.ws_write_tx.replace(tx.clone())
        };

        // Lifecycle observers see Open before either I/O actor can emit a
        // frame or transport failure for this physical session.
        self.observe_lifecycle(
            &conn.id,
            session_generation,
            path_scope.as_deref(),
            WebsocketLifecycleEvent::Open,
        );

        {
            let wconn = conn.clone();
            let common_clone = self.clone();
            let writer_url = url.to_string();
            let writer_path_scope = path_scope.clone();

            let writer_task = spawn(async move {
                let mut sink = write_half;
                while let Some(msg) = rx.recv().await {
                    if let Err(e) = sink.send(msg).await {
                        let failure_reason =
                            WebsocketConnectionFailureReason::from_tungstenite_error(&e);

                        error!(
                            "Write error on {}, classified as {:?}",
                            wconn.id, failure_reason
                        );
                        common_clone.observe_lifecycle(
                            &wconn.id,
                            session_generation,
                            writer_path_scope.as_deref(),
                            WebsocketLifecycleEvent::WriteError,
                        );

                        // Apply same reconnection logic as reader errors
                        let mut conn_state = wconn.state.lock().await;
                        if !conn_state.close_initiated
                            && !conn_state.reconnection_pending
                            && wconn.session_generation.load(Ordering::Acquire)
                                == session_generation
                            && failure_reason.should_reconnect()
                        {
                            info!(
                                "Writer connection {} has recoverable error, attempting reconnection: {:?}",
                                wconn.id, failure_reason
                            );
                            conn_state.reconnection_pending = true;
                            conn_state.is_session_logged_on = false;
                            conn_state.ws_write_tx = None;
                            conn_state.writer_session_generation = None;
                            drop(conn_state);
                            let reconnect_url = common_clone
                                .get_reconnect_url(&writer_url, Arc::clone(&wconn))
                                .await;

                            let _ = common_clone
                                .reconnect_tx
                                .send(ReconnectEntry {
                                    connection_id: wconn.id.clone(),
                                    session_generation,
                                    path_scope: writer_path_scope.clone(),
                                    url: reconnect_url,
                                    is_renewal: false,
                                })
                                .await;
                        } else {
                            warn!(
                                "Writer connection {} will not schedule another reconnect: {:?}",
                                wconn.id, failure_reason
                            );
                        }

                        break;
                    }
                }
                debug!("Writer {} exit", wconn.id);
            });
            self.register_background_task(writer_task);
        }

        {
            let common = self.clone();
            let reader_conn = conn.clone();
            let read_url = url.to_string();
            let reader_path_scope = path_scope.clone();

            let reader_task = spawn(async move {
                let mut stream_end_reason = None;
                while let Some(item) = read_half.next().await {
                    match item {
                        Ok(Message::Text(msg)) => {
                            common
                                .on_text_frame(
                                    msg.to_string(),
                                    Arc::clone(&reader_conn),
                                    session_generation,
                                    reader_path_scope.as_deref(),
                                )
                                .await;
                        }
                        Ok(Message::Binary(bin)) => {
                            common
                                .on_binary_frame(
                                    &bin,
                                    Arc::clone(&reader_conn),
                                    session_generation,
                                    reader_path_scope.as_deref(),
                                )
                                .await;
                        }
                        Ok(Message::Ping(payload)) => {
                            info!("PING received from server on {}", reader_conn.id);
                            common.observe_lifecycle(
                                &reader_conn.id,
                                session_generation,
                                reader_path_scope.as_deref(),
                                WebsocketLifecycleEvent::Ping,
                            );
                            common.events.emit(&WebsocketEvent::Ping);
                            if let Some(tx) = reader_conn.state.lock().await.ws_write_tx.clone() {
                                let _ = tx.send(Message::Pong(payload));
                                info!(
                                    "Responded PONG to server's PING message on {}",
                                    reader_conn.id
                                );
                            }
                        }
                        Ok(Message::Pong(_)) => {
                            info!("Received PONG from server on {}", reader_conn.id);
                            common.observe_lifecycle(
                                &reader_conn.id,
                                session_generation,
                                reader_path_scope.as_deref(),
                                WebsocketLifecycleEvent::Pong,
                            );
                            common.events.emit(&WebsocketEvent::Pong);
                        }
                        Ok(Message::Close(frame)) => {
                            let (code, reason) = frame
                                .map_or((1000, String::new()), |CloseFrame { code, reason }| {
                                    (code.into(), reason.to_string())
                                });
                            common.observe_lifecycle(
                                &reader_conn.id,
                                session_generation,
                                reader_path_scope.as_deref(),
                                WebsocketLifecycleEvent::Close { code },
                            );
                            common
                                .events
                                .emit(&WebsocketEvent::Close(code, reason.clone()));

                            // Classify the close reason
                            let user_initiated = {
                                let conn_state = reader_conn.state.lock().await;
                                conn_state.close_initiated
                            };

                            let failure_reason = WebsocketConnectionFailureReason::from_close_code(
                                code,
                                user_initiated,
                            );
                            stream_end_reason = Some(failure_reason);

                            info!(
                                "Connection {} received close frame: code={}, classified as {:?}",
                                reader_conn.id, code, failure_reason
                            );

                            let mut conn_state = reader_conn.state.lock().await;
                            if !conn_state.close_initiated
                                && !conn_state.reconnection_pending
                                && reader_conn.session_generation.load(Ordering::Acquire)
                                    == session_generation
                                && failure_reason.should_reconnect()
                            {
                                info!(
                                    "Connection {} received close frame with reconnectable failure: {:?}",
                                    reader_conn.id, failure_reason
                                );
                                conn_state.reconnection_pending = true;
                                conn_state.is_session_logged_on = false;
                                conn_state.ws_write_tx = None;
                                conn_state.writer_session_generation = None;
                                drop(conn_state);
                                let reconnect_url = common
                                    .get_reconnect_url(&read_url, Arc::clone(&reader_conn))
                                    .await;

                                let _ = common
                                    .reconnect_tx
                                    .send(ReconnectEntry {
                                        connection_id: reader_conn.id.clone(),
                                        session_generation,
                                        path_scope: reader_path_scope.clone(),
                                        url: reconnect_url,
                                        is_renewal: false,
                                    })
                                    .await;
                            } else {
                                warn!(
                                    "Connection {} close frame will not schedule another reconnect: {:?}",
                                    reader_conn.id, failure_reason
                                );
                                if matches!(
                                    failure_reason,
                                    WebsocketConnectionFailureReason::AuthenticationFailure
                                        | WebsocketConnectionFailureReason::ProtocolViolation
                                        | WebsocketConnectionFailureReason::ConfigurationError
                                        | WebsocketConnectionFailureReason::PermanentServerError
                                ) {
                                    common.events.emit(&WebsocketEvent::Error(format!(
                                        "[CRITICAL] Connection {} permanently failed: {:?}",
                                        reader_conn.id, failure_reason
                                    )));
                                }
                            }

                            break;
                        }
                        Err(e) => {
                            // Classify the error type for reconnection decision
                            let failure_reason =
                                WebsocketConnectionFailureReason::from_tungstenite_error(&e);

                            stream_end_reason = Some(failure_reason);
                            error!(
                                "WebSocket error on {}, classified as {:?}",
                                reader_conn.id, failure_reason
                            );
                            common.observe_lifecycle(
                                &reader_conn.id,
                                session_generation,
                                reader_path_scope.as_deref(),
                                WebsocketLifecycleEvent::ReadError,
                            );

                            common.events.emit(&WebsocketEvent::Error(format!(
                                "WebSocket transport error: {failure_reason:?}"
                            )));

                            // Apply the same reconnection logic as Close frames
                            let mut conn_state = reader_conn.state.lock().await;
                            if !conn_state.close_initiated
                                && !conn_state.reconnection_pending
                                && reader_conn.session_generation.load(Ordering::Acquire)
                                    == session_generation
                                && failure_reason.should_reconnect()
                            {
                                info!(
                                    "Connection {} has recoverable error, attempting reconnection: {:?}",
                                    reader_conn.id, failure_reason
                                );
                                conn_state.reconnection_pending = true;
                                conn_state.is_session_logged_on = false;
                                conn_state.ws_write_tx = None;
                                conn_state.writer_session_generation = None;
                                drop(conn_state);
                                let reconnect_url = common
                                    .get_reconnect_url(&read_url, Arc::clone(&reader_conn))
                                    .await;

                                let _ = common
                                    .reconnect_tx
                                    .send(ReconnectEntry {
                                        connection_id: reader_conn.id.clone(),
                                        session_generation,
                                        path_scope: reader_path_scope.clone(),
                                        url: reconnect_url,
                                        is_renewal: false,
                                    })
                                    .await;
                            } else {
                                warn!(
                                    "Connection {} will not schedule another reconnect: {:?}",
                                    reader_conn.id, failure_reason
                                );
                                if matches!(
                                    failure_reason,
                                    WebsocketConnectionFailureReason::AuthenticationFailure
                                        | WebsocketConnectionFailureReason::ProtocolViolation
                                        | WebsocketConnectionFailureReason::ConfigurationError
                                        | WebsocketConnectionFailureReason::PermanentServerError
                                ) {
                                    common.events.emit(&WebsocketEvent::Error(format!(
                                        "[CRITICAL] Connection {} permanently failed: {:?}",
                                        reader_conn.id, failure_reason
                                    )));
                                }
                            }

                            break;
                        }
                        _ => {}
                    }
                }

                // Handle case where stream ends unexpectedly (e.g., network disconnection)
                info!("WebSocket stream ended for connection {}", reader_conn.id);
                if stream_end_reason.is_none() {
                    common.observe_lifecycle(
                        &reader_conn.id,
                        session_generation,
                        reader_path_scope.as_deref(),
                        WebsocketLifecycleEvent::StreamEnded,
                    );
                }

                // Handle possibly unexpected stream end with same logic as other errors
                let failure_reason =
                    stream_end_reason.unwrap_or(WebsocketConnectionFailureReason::StreamEnded);

                info!(
                    "WebSocket stream ended for connection {}, classified as {:?}",
                    reader_conn.id, failure_reason
                );

                let mut conn_state = reader_conn.state.lock().await;
                if !conn_state.close_initiated
                    && !conn_state.reconnection_pending
                    && reader_conn.session_generation.load(Ordering::Acquire) == session_generation
                    && failure_reason.should_reconnect()
                {
                    info!(
                        "Connection {} stream ended unexpectedly, attempting reconnection",
                        reader_conn.id
                    );
                    conn_state.reconnection_pending = true;
                    conn_state.is_session_logged_on = false;
                    conn_state.ws_write_tx = None;
                    conn_state.writer_session_generation = None;
                    drop(conn_state);
                    let reconnect_url = common
                        .get_reconnect_url(&read_url, Arc::clone(&reader_conn))
                        .await;

                    let _ = common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: reader_conn.id.clone(),
                            session_generation,
                            path_scope: reader_path_scope,
                            url: reconnect_url,
                            is_renewal: false,
                        })
                        .await;
                } else {
                    debug!(
                        "Connection {} stream ended normally (close_initiated={}, is_renewal={})",
                        reader_conn.id, conn_state.close_initiated, is_renewal
                    );
                }

                debug!("Reader actor for {} exiting", reader_conn.id);
            });
            self.register_background_task(reader_task);
        }

        // Keep the per-slot init lock until generated on-open work has
        // observed this session. Reader/writer actors are already running, so
        // handlers may send a request and await its response without blocking
        // the transport.
        self.on_open(url.to_string(), Arc::clone(&conn), old_writer)
            .await;

        Ok(())
    }
    /// Gracefully disconnects all active WebSocket connections.
    ///
    /// This method attempts to close all connections in the connection pool within a 30-second timeout.
    /// It marks each connection as close-initiated and attempts to close them gracefully.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if all connections are successfully closed
    /// - `Err(WebsocketError)` if there are errors during disconnection or a timeout occurs
    ///
    /// # Errors
    ///
    /// Returns `WebsocketError::Timeout` if disconnection takes longer than 30 seconds
    ///
    async fn disconnect(&self) -> Result<(), WebsocketError> {
        if !self.is_connected(None).await {
            warn!("No active connection to close.");
            return Ok(());
        }

        let mut shutdowns = FuturesUnordered::new();
        for conn in &self.connection_pool {
            {
                let mut conn_state = conn.state.lock().await;
                conn_state.close_initiated = true;
                if let Some(tx) = &conn_state.ws_write_tx {
                    shutdowns.push(self.close_connection_gracefully(tx.clone(), Arc::clone(conn)));
                }
            }
        }

        let close_all = async {
            while let Some(result) = shutdowns.next().await {
                result?;
            }
            Ok::<(), WebsocketError>(())
        };

        match timeout(Duration::from_secs(30), close_all).await {
            Ok(Ok(())) => {
                info!("Disconnected all WebSocket connections successfully.");
                for conn in &self.connection_pool {
                    let mut st = conn.state.lock().await;
                    st.is_session_logged_on = false;
                    st.session_logon_req = None;
                }
                Ok(())
            }
            Ok(Err(err)) => {
                error!("Error while disconnecting: {:?}", err);
                Err(err)
            }
            Err(_) => {
                error!("Timed out while disconnecting WebSocket connections.");
                Err(WebsocketError::Timeout)
            }
        }
    }

    /// Sends a PING message to all ready WebSocket connections.
    ///
    /// This method iterates through the connection pool, identifies ready connections,
    /// and sends a PING message to each of them. It logs the number of connections
    /// being pinged and handles any send errors individually.
    ///
    /// # Behavior
    ///
    /// - Skips connections that are not ready
    /// - Logs a warning if no connections are ready
    /// - Sends PING messages concurrently
    /// - Logs debug/error messages for each PING attempt
    async fn ping_server(&self) {
        let mut ready = Vec::new();
        for conn in &self.connection_pool {
            if self.is_connection_ready(conn, false).await {
                let id = conn.id.clone();
                let ws_write_tx = {
                    let conn_state = conn.state.lock().await;
                    conn_state.ws_write_tx.clone()
                };
                ready.push((id, ws_write_tx));
            }
        }

        if ready.is_empty() {
            warn!("No ready connections for PING.");
            return;
        }
        info!("Sending PING to {} WebSocket connections.", ready.len());

        let mut tasks = FuturesUnordered::new();
        for (id, ws_write_tx_opt) in ready {
            if let Some(tx) = ws_write_tx_opt {
                tasks.push(async move {
                    if let Err(e) = tx.send(Message::Ping(Vec::new().into())) {
                        error!("Failed to send PING to {}: {:?}", id, e);
                    } else {
                        debug!("Sent PING to connection {}", id);
                    }
                });
            } else {
                error!("Connection {} was ready but has no write channel", id);
            }
        }

        while tasks.next().await.is_some() {}
    }

    /// Sends a WebSocket message and optionally waits for a reply.
    ///
    /// # Arguments
    ///
    /// * `payload` - The message payload to send
    /// * `id` - Optional request identifier, required when waiting for a reply
    /// * `wait_for_reply` - Whether to wait for a response to the message
    /// * `timeout` - Maximum duration to wait for a reply
    /// * `connection` - Optional specific WebSocket connection to use
    ///
    /// # Returns
    ///
    /// A receiver for the response if `wait_for_reply` is true, otherwise `None`
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if the connection is not ready or the send fails
    async fn send(
        &self,
        payload: String,
        id: Option<String>,
        wait_for_reply: bool,
        timeout: Duration,
        connection: Option<Arc<WebsocketConnection>>,
    ) -> Result<Option<oneshot::Receiver<Result<Value, WebsocketError>>>, WebsocketError> {
        let conn = if let Some(c) = connection {
            c
        } else {
            self.get_connection(false, None).await?
        };

        if !self.is_connected(Some(&conn)).await {
            warn!("Send attempted on a non-connected socket");
            return Err(WebsocketError::NotConnected);
        }

        let ws_write_tx = {
            let conn_state = conn.state.lock().await;
            conn_state
                .ws_write_tx
                .clone()
                .ok_or(WebsocketError::NotConnected)?
        };

        let pending_setup = if wait_for_reply {
            let request_id = id.ok_or_else(|| {
                error!("id is required when waiting for a reply");
                WebsocketError::NotConnected
            })?;

            let (tx, rx) = oneshot::channel();
            {
                let mut conn_state = conn.state.lock().await;
                conn_state
                    .pending_requests
                    .insert(request_id.clone(), PendingRequest { completion: tx });
            }

            Some((request_id, rx))
        } else {
            None
        };

        debug!("Sending message to WebSocket on connection {}", conn.id);

        if ws_write_tx
            .send(Message::Text(payload.clone().into()))
            .is_err()
        {
            if let Some((request_id, _)) = &pending_setup {
                let mut conn_state = conn.state.lock().await;
                conn_state.pending_requests.remove(request_id);
            }
            return Err(WebsocketError::NotConnected);
        }

        let rx = if let Some((request_id, rx)) = pending_setup {
            let conn_clone = Arc::clone(&conn);
            let timeout_id = request_id.clone();
            spawn(async move {
                sleep(timeout).await;
                let mut conn_state = conn_clone.state.lock().await;
                if let Some(pending_req) = conn_state.pending_requests.remove(&timeout_id) {
                    let _ = pending_req.completion.send(Err(WebsocketError::Timeout));
                }
            });
            Some(rx)
        } else {
            None
        };

        Ok(rx)
    }
}

#[derive(Debug, Default, Clone)]
pub struct WebsocketMessageSendOptions {
    pub with_api_key: bool,
    pub is_signed: bool,
    pub is_session_logon: Option<bool>,
    pub is_session_logout: Option<bool>,
}

impl WebsocketMessageSendOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_api_key(mut self) -> Self {
        self.with_api_key = true;
        self
    }

    #[must_use]
    pub fn signed(mut self) -> Self {
        self.is_signed = true;
        self
    }

    #[must_use]
    pub fn session_logon(mut self) -> Self {
        self.is_session_logon = Some(true);
        self
    }

    #[must_use]
    pub fn session_logout(mut self) -> Self {
        self.is_session_logout = Some(true);
        self
    }
}

#[derive(Debug)]
pub enum SendWebsocketMessageResult<R> {
    Single(WebsocketApiResponse<R>),
    Multiple(Vec<WebsocketApiResponse<R>>),
}

impl<R> IntoIterator for SendWebsocketMessageResult<R> {
    type Item = WebsocketApiResponse<R>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            SendWebsocketMessageResult::Single(resp) => vec![resp].into_iter(),
            SendWebsocketMessageResult::Multiple(v) => v.into_iter(),
        }
    }
}

pub struct WebsocketApi {
    pub common: Arc<WebsocketCommon>,
    configuration: ConfigurationWebsocketApi,
    is_connecting: Arc<Mutex<bool>>,
    stream_callbacks: Mutex<HashMap<String, Vec<Arc<dyn Fn(&Value) + Send + Sync + 'static>>>>,
}

impl WebsocketApi {
    #[must_use]
    /// Creates a new WebSocket API instance with the given configuration and connection pool.
    ///
    /// # Arguments
    ///
    /// * `configuration` - Configuration settings for the WebSocket API
    /// * `connection_pool` - A vector of WebSocket connections to be used
    ///
    /// # Returns
    ///
    /// An `Arc`-wrapped `WebsocketApi` instance ready for use
    ///
    /// # Panics
    ///
    /// This function will panic if the configuration is not valid.
    ///
    /// # Examples
    ///
    ///
    /// let api = `WebsocketApi::new(config`, `connection_pool`);
    ///
    pub fn new(
        configuration: ConfigurationWebsocketApi,
        connection_pool: Vec<Arc<WebsocketConnection>>,
    ) -> Arc<Self> {
        let agent_clone = configuration.agent.clone();
        let user_agent_clone = configuration.user_agent.clone();
        let common = WebsocketCommon::new(
            connection_pool,
            configuration.mode.clone(),
            usize::try_from(configuration.reconnect_delay)
                .expect("reconnect_delay should fit in usize"),
            agent_clone,
            Some(user_agent_clone),
        );

        Arc::new(Self {
            common: Arc::clone(&common),
            configuration,
            is_connecting: Arc::new(Mutex::new(false)),
            stream_callbacks: Mutex::new(HashMap::new()),
        })
    }

    /// Connects to a WebSocket server with a configurable timeout and connection handling.
    ///
    /// This method attempts to establish a WebSocket connection if not already connected.
    /// It prevents multiple simultaneous connection attempts and supports a connection pool.
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if:
    /// - Connection fails
    /// - Connection times out after 10 seconds
    ///
    /// # Behavior
    ///
    /// - Checks if already connected and returns early if so
    /// - Prevents multiple concurrent connection attempts
    /// - Sets a WebSocket handler for the connection pool
    /// - Attempts to connect with a 10-second timeout
    ///
    /// # Returns
    ///
    /// `Ok(())` if connection is successful, otherwise a `WebsocketError`
    pub async fn connect(self: Arc<Self>) -> Result<(), WebsocketError> {
        if self.common.is_connected(None).await {
            info!("WebSocket connection already established");
            return Ok(());
        }

        {
            let mut flag = self.is_connecting.lock().await;
            if *flag {
                info!("Already connecting...");
                return Ok(());
            }
            *flag = true;
        }

        let url = self.prepare_url(self.configuration.ws_url.as_deref().unwrap_or_default());

        let handler: Arc<dyn WebsocketHandler> = self.clone();
        for slot in &self.common.connection_pool {
            slot.set_handler(handler.clone()).await;
        }

        let result = select! {
            () = sleep(Duration::from_secs(10)) => Err(WebsocketError::Timeout),
            r = self.common.clone().connect_pool(&url, None) => r,
        };

        {
            let mut flag = self.is_connecting.lock().await;
            *flag = false;
        }

        result
    }

    /// Disconnects the WebSocket connection.
    ///
    /// # Returns
    ///
    /// `Ok(())` if disconnection is successful, otherwise a `WebsocketError`
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if:
    /// - Disconnection fails
    /// - Connection is not established
    ///
    pub async fn disconnect(&self) -> Result<(), WebsocketError> {
        self.common.disconnect().await
    }

    /// Checks if the WebSocket connection is currently established.
    ///
    /// # Returns
    ///
    /// `true` if the connection is active, `false` otherwise.
    pub async fn is_connected(&self) -> bool {
        self.common.is_connected(None).await
    }

    /// Sends a ping to the WebSocket server to maintain the connection.
    ///
    /// This method calls the underlying connection's ping mechanism to check
    /// and keep the WebSocket connection alive.
    pub async fn ping_server(&self) {
        self.common.ping_server().await;
    }

    /// Sends a WebSocket message with the specified method and payload.
    ///
    /// This method prepares and sends a WebSocket request with optional API key and signature.
    /// It handles connection status, generates a unique request ID, and processes the response.
    ///
    /// # Arguments
    ///
    /// * `method` - The WebSocket API method to be called
    /// * `payload` - A map of parameters to be sent with the request
    /// * `options` - Configuration options for message sending (API key, signing)
    ///
    /// # Returns
    ///
    /// A deserialized response of type `R` or a `WebsocketError` if the request fails
    ///
    /// # Panics
    ///
    /// Panics if:
    ///
    /// - The WebSocket is not connected
    /// - The request cannot be processed
    /// - No response is received within the timeout
    ///
    /// # Errors
    ///
    /// Returns `WebsocketError` if:
    /// - The WebSocket is not connected
    /// - The request cannot be processed
    /// - No response is received within the timeout
    pub async fn send_message<R>(
        &self,
        method: &str,
        payload: BTreeMap<String, Value>,
        options: WebsocketMessageSendOptions,
    ) -> Result<SendWebsocketMessageResult<R>, WebsocketError>
    where
        R: DeserializeOwned + Send + Sync + 'static,
    {
        if !self.common.is_connected(None).await {
            return Err(WebsocketError::NotConnected);
        }

        let do_multi =
            options.is_session_logon.unwrap_or(false) || options.is_session_logout.unwrap_or(false);

        let connections = if do_multi {
            self.common.get_available_connections(false, None).await
        } else {
            vec![self.common.get_connection(false, None).await?]
        };

        let skip_auth = if do_multi {
            false
        } else {
            let connection = &connections[0];
            let conn_state = connection.state.lock().await;
            self.configuration.auto_session_relogon && conn_state.is_session_logged_on
        };

        let payload_clone = payload.clone();

        let (id, request) =
            build_websocket_api_message(&self.configuration, method, payload, &options, skip_auth);
        let raw_payload = serde_json::to_string(&request).unwrap();
        debug!("Sending WebSocket API method {method} with request id {id}");

        let timeout = Duration::from_millis(self.configuration.timeout);

        let mut receivers = Vec::with_capacity(connections.len());
        for connection in &connections {
            let opt_rx = self
                .common
                .send(
                    raw_payload.clone(),
                    Some(id.clone()),
                    true,
                    timeout,
                    Some(connection.clone()),
                )
                .await?;
            receivers.push((connection.clone(), opt_rx));
        }

        let mut raw_msgs = Vec::with_capacity(receivers.len());
        for (_conn, opt_rx) in receivers {
            let rx = opt_rx.ok_or(WebsocketError::NoResponse)?;
            let msg = rx.await.unwrap_or(Err(WebsocketError::Timeout))?;
            raw_msgs.push(msg);
        }

        let mut responses = Vec::with_capacity(raw_msgs.len());
        for msg in raw_msgs {
            let raw = msg
                .get("result")
                .or_else(|| msg.get("response"))
                .cloned()
                .unwrap_or(Value::Null);

            let rate_limits = msg
                .get("rateLimits")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| serde_json::from_value(v.clone()).ok())
                        .collect()
                })
                .unwrap_or_default();

            responses.push(WebsocketApiResponse {
                raw,
                rate_limits,
                _marker: PhantomData,
            });
        }

        if do_multi && self.configuration.auto_session_relogon {
            for connection in &connections {
                let mut state = connection.state.lock().await;
                if options.is_session_logon.unwrap_or(false) {
                    state.is_session_logged_on = true;
                    state.session_logon_req = Some(WebsocketSessionLogonReq {
                        method: method.to_string(),
                        payload: payload_clone.clone(),
                        options: options.clone(),
                    });
                } else {
                    state.is_session_logged_on = false;
                    state.session_logon_req = None;
                }
            }
        }

        Ok(if responses.len() == 1 && !do_multi {
            SendWebsocketMessageResult::Single(responses.into_iter().next().unwrap())
        } else {
            SendWebsocketMessageResult::Multiple(responses)
        })
    }

    /// Prepares a WebSocket URL by appending a validated time unit parameter.
    ///
    /// This method checks if a time unit is configured and validates it. If valid,
    /// the time unit is appended to the URL as a query parameter. If no time unit
    /// is specified or the validation fails, the original URL is returned.
    ///
    /// # Arguments
    ///
    /// * `ws_url` - The base WebSocket URL to be modified
    ///
    /// # Returns
    ///
    /// A modified URL with the time unit parameter, or the original URL if no
    /// modification is possible
    fn prepare_url(&self, ws_url: &str) -> String {
        let mut url = ws_url.to_string();

        let time_unit = match &self.configuration.time_unit {
            Some(u) => u.to_string(),
            None => return url,
        };

        match validate_time_unit(&time_unit) {
            Ok(Some(validated)) => {
                let sep = if url.contains('?') { '&' } else { '?' };
                url.push(sep);
                url.push_str("timeUnit=");
                url.push_str(validated);
            }
            Ok(None) => {}
            Err(e) => {
                error!("Invalid time unit provided: {:?}", e);
            }
        }

        url
    }
}

#[async_trait]
impl WebsocketHandler for WebsocketApi {
    /// Handles the WebSocket connection opening event, attempting to re-establish a session logon if needed.
    ///
    /// This method checks if a session logon request exists and has not already been logged on.
    /// If conditions are met, it attempts to send a session re-logon message and update the connection state.
    ///
    /// # Arguments
    ///
    /// * `_url` - The WebSocket connection URL (unused)
    /// * `connection` - The WebSocket connection context
    ///
    /// # Behavior
    ///
    /// - Checks for an existing session logon request
    /// - Verifies the session is not already logged on
    /// - Attempts to send a re-logon message
    /// - Updates connection state upon successful re-logon
    /// - Logs errors if re-logon dispatch fails
    async fn on_open(&self, _url: String, connection: Arc<WebsocketConnection>) {
        let session_req = {
            let conn_state = connection.state.lock().await;
            conn_state.session_logon_req.clone()
        };

        let Some(req) = session_req else {
            return;
        };

        let already_logged_on = {
            let conn_state = connection.state.lock().await;
            conn_state.is_session_logged_on
        };

        if already_logged_on {
            debug!(
                "Connection {} already logged on, skipping re-logon",
                connection.id
            );
            return;
        }

        let conn = connection.clone();
        let common = Arc::clone(&self.common);
        let configuration = self.configuration.clone();
        let method = req.method.clone();
        let payload = req.payload.clone();
        let options = req.options.clone();

        spawn(async move {
            let (id, json_msg) =
                build_websocket_api_message(&configuration, &method, payload, &options, false);

            let raw_message = match serde_json::to_string(&json_msg) {
                Ok(msg) => msg,
                Err(e) => {
                    warn!(
                        "Failed to serialize session logon message for connection {}: {}",
                        conn.id, e
                    );
                    return;
                }
            };

            debug!(
                "Session re-logon method {} on connection {} with request id {}",
                method, conn.id, id
            );

            let rx = match common
                .send(
                    raw_message,
                    Some(id.clone()),
                    true,
                    Duration::from_millis(configuration.timeout),
                    Some(conn.clone()),
                )
                .await
            {
                Ok(Some(rx)) => rx,
                Ok(None) => {
                    warn!(
                        "Session re-logon dispatch returned None for connection {}",
                        conn.id
                    );
                    return;
                }
                Err(_) => {
                    warn!("Session re-logon dispatch failed on connection {}", conn.id);
                    return;
                }
            };

            let Ok(result) = timeout(Duration::from_millis(configuration.timeout), rx).await else {
                warn!("Session re-logon timed out on connection {}", conn.id);
                return;
            };

            let Ok(final_result) = result else {
                warn!("Session re-logon receiver closed on connection {}", conn.id);
                return;
            };

            if final_result.is_err() {
                warn!("Session re-logon rejected on connection {}", conn.id);
                return;
            }

            debug!("Session re-logon succeeded on connection {}", conn.id);
            let mut conn_state = conn.state.lock().await;
            conn_state.is_session_logged_on = true;
        });
    }

    /// Handles incoming WebSocket messages by parsing the JSON payload and processing pending requests.
    ///
    /// This method is responsible for:
    /// - Parsing the received WebSocket message as JSON
    /// - Matching the message to a pending request by its ID
    /// - Sending the response back to the original request's completion channel
    /// - Handling both successful and error responses
    ///
    /// # Arguments
    ///
    /// * `data` - The raw WebSocket message as a string
    /// * `connection` - The WebSocket connection context associated with the message
    ///
    /// # Behavior
    ///
    /// - If message parsing fails, logs an error and returns
    /// - For known request IDs, sends the response to the corresponding completion channel
    /// - Warns about responses for unknown or timed-out requests
    /// - Differentiates between successful (status < 400) and error responses
    async fn on_message(&self, data: String, connection: Arc<WebsocketConnection>) {
        let msg: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(err) => {
                error!("Failed to parse WebSocket API message: {}", err);
                return;
            }
        };

        if let Some(id) = msg.get("id").and_then(Value::as_str) {
            let maybe_sender = {
                let mut conn_state = connection.state.lock().await;
                conn_state.pending_requests.remove(id)
            };

            if let Some(PendingRequest { completion }) = maybe_sender {
                connection.drain_notify.notify_one();
                let status = msg.get("status").and_then(Value::as_u64).unwrap_or(200);
                if status >= 400 {
                    let error_map = msg
                        .get("error")
                        .and_then(Value::as_object)
                        .unwrap_or(&serde_json::Map::new())
                        .clone();

                    let code = error_map
                        .get("code")
                        .and_then(Value::as_i64)
                        .unwrap_or(status.try_into().unwrap());

                    let message = error_map
                        .get("msg")
                        .and_then(Value::as_str)
                        .unwrap_or("Unknown error")
                        .to_string();

                    let _ = completion.send(Err(WebsocketError::ResponseError { code, message }));
                } else {
                    let _ = completion.send(Ok(msg.clone()));
                }
            }

            return;
        }

        if let Some(event) = msg.get("event") {
            if let Some(event_type) = event.get("e").and_then(Value::as_str) {
                if event_type == "serverShutdown" {
                    warn!(
                        "Received serverShutdown event on connection {}",
                        connection.id
                    );

                    let mut conn_state = connection.state.lock().await;

                    if !conn_state.renewal_pending && !conn_state.close_initiated {
                        conn_state.renewal_pending = true;

                        let path_scope = conn_state.url_path.clone();
                        let url = path_scope.clone().unwrap_or_default();

                        drop(conn_state);

                        if self
                            .common
                            .reconnect_tx
                            .send(ReconnectEntry {
                                connection_id: connection.id.clone(),
                                session_generation: connection
                                    .session_generation
                                    .load(Ordering::Acquire),
                                path_scope,
                                url,
                                is_renewal: true,
                            })
                            .await
                            .is_err()
                        {
                            error!("Failed to enqueue serverShutdown renewal");
                        }
                    }

                    return;
                }
            }
        }

        if let Some(event) = msg.get("event") {
            if event.get("e").is_some() {
                for callbacks in self.stream_callbacks.lock().await.values() {
                    for callback in callbacks {
                        callback(event);
                    }
                }

                return;
            }
        }

        warn!(
            "Received uncorrelated WebSocket API payload on connection {}",
            connection.id
        );
    }

    /// Generates the URL to use for reconnecting to a WebSocket connection.
    ///
    /// # Arguments
    ///
    /// * `default_url` - The original URL to potentially modify for reconnection
    /// * `_connection` - The WebSocket connection context (currently unused)
    ///
    /// # Returns
    ///
    /// A `String` representing the URL to use for reconnecting
    async fn get_reconnect_url(
        &self,
        default_url: String,
        _connection: Arc<WebsocketConnection>,
    ) -> String {
        default_url
    }
}

pub struct WebsocketStreams {
    pub common: Arc<WebsocketCommon>,
    pub stream_id_is_strictly_number: AtomicBool,
    next_stream_request_id: AtomicU32,
    next_stream_pending_token: AtomicU64,
    next_stream_reservation_token: AtomicU64,
    stream_subscription_events: StreamSubscriptionEventEmitter,
    url_paths: Vec<String>,
    is_connecting: Mutex<bool>,
    connection_streams: Arc<Mutex<HashMap<String, Arc<WebsocketConnection>>>>,
    configuration: ConfigurationWebsocketStreams,
}

async fn release_unconfirmed_stream_keys(
    connection_streams: Arc<Mutex<HashMap<String, Arc<WebsocketConnection>>>>,
    connection: Arc<WebsocketConnection>,
    stream_keys: &[String],
    reservation_token: u64,
) {
    let mut assignments = connection_streams.lock().await;
    let mut state = connection.state.lock().await;
    let path_scope = state.url_path.clone();
    let mut released_keys = Vec::new();

    for key in stream_keys {
        let owned_here = assignments
            .get(key)
            .is_some_and(|assigned| Arc::ptr_eq(assigned, &connection));
        let owns_reservation = state
            .unconfirmed_stream_reservations
            .get(key)
            .is_some_and(|current| *current == reservation_token);
        if owned_here && owns_reservation {
            state.stream_callbacks.remove(key);
            state.confirmed_stream_keys.remove(key);
            state.unconfirmed_stream_reservations.remove(key);
            assignments.remove(key);
            released_keys.push(key.clone());
        }
    }

    state.pending_subscriptions.retain(|stream| {
        let key = match path_scope.as_deref() {
            Some(path) if !path.is_empty() => format!("{path}::{stream}"),
            _ => stream.clone(),
        };
        !released_keys.contains(&key)
    });
}

fn spawn_stream_terminal_owner(
    connection_streams: Arc<Mutex<HashMap<String, Arc<WebsocketConnection>>>>,
    connection: Arc<WebsocketConnection>,
    stream_keys: Vec<String>,
    reservation_token: Option<u64>,
    events: StreamSubscriptionEventEmitter,
    context: StreamSubscriptionContext,
    outcome: StreamSubscriptionOutcome,
    terminal_claimed: bool,
) -> JoinHandle<()> {
    spawn(async move {
        if let Some(reservation_token) = reservation_token {
            release_unconfirmed_stream_keys(
                connection_streams,
                connection,
                &stream_keys,
                reservation_token,
            )
            .await;
        }
        if terminal_claimed {
            StreamSubscriptionLifecycle::emit_claimed(&events, context, outcome);
        }
    })
}

async fn finish_stream_terminal(
    connection_streams: Arc<Mutex<HashMap<String, Arc<WebsocketConnection>>>>,
    connection: Arc<WebsocketConnection>,
    stream_keys: Vec<String>,
    reservation_token: Option<u64>,
    events: StreamSubscriptionEventEmitter,
    context: StreamSubscriptionContext,
    outcome: StreamSubscriptionOutcome,
    terminal_claimed: bool,
) {
    let connection_id = context.connection_id.clone();
    let request_id = context.request_id;
    if let Err(join_error) = spawn_stream_terminal_owner(
        connection_streams,
        connection,
        stream_keys,
        reservation_token,
        events,
        context,
        outcome,
        terminal_claimed,
    )
    .await
    {
        error!(
            "Stream subscription terminal owner failed on connection {} request {}: {}",
            connection_id, request_id, join_error
        );
    }
}

struct UnconfirmedAssignmentGuard {
    connection_streams: Arc<Mutex<HashMap<String, Arc<WebsocketConnection>>>>,
    connection: Arc<WebsocketConnection>,
    stream_keys: Vec<String>,
    request_id: u32,
    reservation_token: u64,
    lifecycle: Arc<StreamSubscriptionLifecycle>,
    events: StreamSubscriptionEventEmitter,
    armed: bool,
}

#[derive(Debug, Clone, Copy)]
enum ConfirmedStreamControlMethod {
    Subscribe,
    #[cfg(feature = "derivatives_trading_usds_futures")]
    Unsubscribe,
}

impl ConfirmedStreamControlMethod {
    fn method(self) -> &'static str {
        match self {
            Self::Subscribe => "SUBSCRIBE",
            #[cfg(feature = "derivatives_trading_usds_futures")]
            Self::Unsubscribe => "UNSUBSCRIBE",
        }
    }
}

enum ConfirmedStreamControlOwnership {
    SdkManaged {
        unconfirmed_reservation_token: Option<u64>,
    },
    #[cfg(feature = "derivatives_trading_usds_futures")]
    CallerManaged { target: RoutedStreamTarget },
}

struct ConfirmedStreamControl<'a> {
    method: ConfirmedStreamControlMethod,
    params: &'a [String],
    ownership: ConfirmedStreamControlOwnership,
}

impl ConfirmedStreamControl<'_> {
    fn context(
        &self,
        connection: &WebsocketConnection,
        state: &WebsocketConnectionState,
        request_id: u32,
    ) -> StreamSubscriptionContext {
        match &self.ownership {
            ConfirmedStreamControlOwnership::SdkManaged { .. } => {
                let session_generation = state
                    .writer_session_generation
                    .unwrap_or_else(|| connection.session_generation.load(Ordering::Acquire));
                StreamSubscriptionContext {
                    connection_id: connection.id.clone(),
                    session_generation,
                    request_id,
                    path_scope: stream_subscription_scope(state.url_path.as_deref()),
                }
            }
            #[cfg(feature = "derivatives_trading_usds_futures")]
            ConfirmedStreamControlOwnership::CallerManaged { target } => {
                target.subscription_context(request_id)
            }
        }
    }

    fn exact_session_is_current(
        &self,
        connection: &WebsocketConnection,
        state: &WebsocketConnectionState,
    ) -> bool {
        #[cfg(not(feature = "derivatives_trading_usds_futures"))]
        let _ = (connection, state);
        match &self.ownership {
            ConfirmedStreamControlOwnership::SdkManaged { .. } => true,
            #[cfg(feature = "derivatives_trading_usds_futures")]
            ConfirmedStreamControlOwnership::CallerManaged { target } => {
                let current_generation = connection.session_generation.load(Ordering::Acquire);
                let writer_generation_mismatch = state
                    .writer_session_generation
                    .is_some_and(|generation| generation != target.session_generation);
                current_generation == target.session_generation
                    && !writer_generation_mismatch
                    && state.url_path.as_deref() == Some(target.path_scope.as_path_scope())
            }
        }
    }

    fn emit_pre_dispatch_terminal(&self) -> bool {
        match &self.ownership {
            ConfirmedStreamControlOwnership::SdkManaged { .. } => false,
            #[cfg(feature = "derivatives_trading_usds_futures")]
            ConfirmedStreamControlOwnership::CallerManaged { .. } => true,
        }
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    fn pending_ownership(&self) -> PendingStreamSubscriptionOwnership {
        match &self.ownership {
            ConfirmedStreamControlOwnership::SdkManaged { .. } => {
                PendingStreamSubscriptionOwnership::SdkManaged
            }
            #[cfg(feature = "derivatives_trading_usds_futures")]
            ConfirmedStreamControlOwnership::CallerManaged { .. } => {
                PendingStreamSubscriptionOwnership::CallerManaged
            }
        }
    }
}

/// Cancellation owner for routed control requests. Unlike
/// [`UnconfirmedAssignmentGuard`], this guard has no desired-stream ownership
/// to release: the application owns its desired set and must reissue it after a
/// newer physical session opens.
#[cfg(feature = "derivatives_trading_usds_futures")]
struct RoutedStreamControlGuard {
    connection: Arc<WebsocketConnection>,
    request_id: u32,
    lifecycle: Arc<StreamSubscriptionLifecycle>,
    events: StreamSubscriptionEventEmitter,
    armed: bool,
}

#[cfg(feature = "derivatives_trading_usds_futures")]
impl RoutedStreamControlGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(feature = "derivatives_trading_usds_futures")]
impl Drop for RoutedStreamControlGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let connection = Arc::clone(&self.connection);
        let request_id = self.request_id;
        let lifecycle = Arc::clone(&self.lifecycle);
        let events = self.events.clone();
        runtime.spawn(async move {
            let cancelled_context = {
                let mut state = connection.state.lock().await;
                let owns_pending = state
                    .pending_stream_subscriptions
                    .get(&request_id)
                    .is_some_and(|pending| Arc::ptr_eq(&pending.lifecycle, &lifecycle));
                if owns_pending {
                    let (pending, terminal_claimed) = state
                        .remove_pending_stream_subscription(request_id, true)
                        .expect("lifecycle-matched routed control must still be pending");
                    terminal_claimed.then_some(pending.context)
                } else {
                    lifecycle.claim_cancelled_if_dispatched()
                }
            };
            if let Some(context) = cancelled_context {
                StreamSubscriptionLifecycle::emit_claimed(
                    &events,
                    context,
                    StreamSubscriptionOutcome::Cancelled,
                );
            }
        });
    }
}

impl UnconfirmedAssignmentGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for UnconfirmedAssignmentGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let connection_streams = Arc::clone(&self.connection_streams);
        let connection = Arc::clone(&self.connection);
        let stream_keys = self.stream_keys.clone();
        let request_id = self.request_id;
        let reservation_token = self.reservation_token;
        let lifecycle = Arc::clone(&self.lifecycle);
        let events = self.events.clone();
        runtime.spawn(async move {
            let (removed_pending, cancelled_context) = {
                let mut state = connection.state.lock().await;
                let owns_pending = state
                    .pending_stream_subscriptions
                    .get(&request_id)
                    .is_some_and(|pending| Arc::ptr_eq(&pending.lifecycle, &lifecycle));
                if owns_pending {
                    let (pending, terminal_claimed) = state
                        .remove_pending_stream_subscription(request_id, true)
                        .expect("lifecycle-matched pending subscription must still exist");
                    let context = terminal_claimed.then(|| pending.context.clone());
                    (Some(pending), context)
                } else {
                    (None, lifecycle.claim_cancelled_if_dispatched())
                }
            };
            let (stream_keys, reservation_token) = removed_pending.map_or_else(
                || (stream_keys, Some(reservation_token)),
                |pending| (pending.stream_keys, pending.unconfirmed_reservation_token),
            );
            if let Some(reservation_token) = reservation_token {
                release_unconfirmed_stream_keys(
                    connection_streams,
                    connection,
                    &stream_keys,
                    reservation_token,
                )
                .await;
            }
            if let Some(context) = cancelled_context {
                StreamSubscriptionLifecycle::emit_claimed(
                    &events,
                    context,
                    StreamSubscriptionOutcome::Cancelled,
                );
            }
        });
    }
}

impl WebsocketStreams {
    /// Creates a new `WebsocketStreams` instance with the given configuration and connection pool.
    ///
    /// # Arguments
    ///
    /// * `configuration` - Configuration settings for the WebSocket streams
    /// * `connection_pool` - A vector of WebSocket connections to use
    /// * `url_paths` - A vector of URL paths for the streams
    ///
    /// # Returns
    ///
    /// An `Arc`-wrapped `WebsocketStreams` instance
    ///
    /// # Panics
    ///
    /// Panics if the `reconnect_delay` cannot be converted to `usize`
    #[must_use]
    pub fn new(
        configuration: ConfigurationWebsocketStreams,
        mut connection_pool: Vec<Arc<WebsocketConnection>>,
        url_paths: Vec<String>,
    ) -> Arc<Self> {
        if !url_paths.is_empty() {
            let base_pool_size = configuration.mode.pool_size();
            let expected = base_pool_size * url_paths.len();

            while connection_pool.len() < expected {
                connection_pool.push(WebsocketConnection::new(random_string()));
            }
        }

        let agent_clone = configuration.agent.clone();
        let user_agent_clone = configuration.user_agent.clone();
        let stream_subscription_observer = configuration.stream_subscription_observer.clone();
        let common = WebsocketCommon::new_with_raw_frame_observer(
            connection_pool,
            configuration.mode.clone(),
            usize::try_from(configuration.reconnect_delay)
                .expect("reconnect_delay should fit in usize"),
            agent_clone,
            Some(user_agent_clone),
            configuration.raw_frame_observer.clone(),
        );
        Arc::new(Self {
            common,
            is_connecting: Mutex::new(false),
            connection_streams: Arc::new(Mutex::new(HashMap::new())),
            configuration,
            stream_id_is_strictly_number: AtomicBool::new(false),
            next_stream_request_id: AtomicU32::new(random_integer()),
            next_stream_pending_token: AtomicU64::new(1),
            next_stream_reservation_token: AtomicU64::new(1),
            stream_subscription_events: StreamSubscriptionEventEmitter::new(
                stream_subscription_observer,
            ),
            url_paths,
        })
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    pub(crate) fn single_connection_identity(&self) -> Option<(String, u64)> {
        let connection = self.common.connection_pool.first()?;
        (self.common.connection_pool.len() == 1).then(|| {
            (
                connection.id.clone(),
                connection.session_generation.load(Ordering::Acquire),
            )
        })
    }

    /// Establishes a WebSocket connection for the given streams.
    ///
    /// This method attempts to connect to a WebSocket server using the connection pool.
    /// If a connection is already established or in progress, it returns immediately.
    ///
    /// # Arguments
    ///
    /// * `streams` - A vector of stream identifiers to connect to
    ///
    /// # Returns
    ///
    /// A `Result` indicating whether the connection was successful or an error occurred
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if the connection fails or times out after 10 seconds
    pub async fn connect(self: Arc<Self>, streams: Vec<String>) -> Result<(), WebsocketError> {
        if self.common.is_connected(None).await {
            info!("WebSocket connection already established");
            return Ok(());
        }

        {
            let mut flag = self.is_connecting.lock().await;
            if *flag {
                info!("Already connecting...");
                return Ok(());
            }
            *flag = true;
        }

        let handler: Arc<dyn WebsocketHandler> = self.clone();
        for conn in &self.common.connection_pool {
            conn.set_handler(handler.clone()).await;
        }

        let base_pool_size = self.configuration.mode.pool_size();

        let connect_fut = async {
            if self.url_paths.is_empty() {
                let url = self.prepare_url(&streams, None);
                self.common.clone().connect_pool(&url, None).await
            } else {
                let mut futures = Vec::with_capacity(self.url_paths.len());

                for (i, path) in self.url_paths.iter().enumerate() {
                    let start = i * base_pool_size;

                    let subset: Vec<Arc<WebsocketConnection>> = self
                        .common
                        .connection_pool
                        .iter()
                        .skip(start)
                        .take(base_pool_size)
                        .cloned()
                        .collect();

                    if subset.len() != base_pool_size {
                        return Err(WebsocketError::ServerError(format!(
                            "connection_pool too small for url_paths: need {} per path, got {} for path index {}",
                            base_pool_size,
                            subset.len(),
                            i
                        )));
                    }

                    for c in &subset {
                        let mut st = c.state.lock().await;
                        st.url_path = Some(path.clone());
                    }

                    let url = self.prepare_url(&streams, Some(path.as_str()));
                    let common = self.common.clone();

                    futures.push(async move { common.connect_pool(&url, Some(subset)).await });
                }

                try_join_all(futures).await?;
                Ok(())
            }
        };

        let connect_res = select! {
            () = sleep(Duration::from_secs(10)) => Err(WebsocketError::Timeout),
            r = connect_fut => r,
        };

        {
            let mut flag = self.is_connecting.lock().await;
            *flag = false;
        }

        connect_res
    }

    /// Disconnects all WebSocket connections and clears associated state.
    ///
    /// # Returns
    ///
    /// A `Result` indicating whether the disconnection was successful or an error occurred
    ///
    /// # Errors
    ///
    /// Returns a `WebsocketError` if there are issues during the disconnection process
    ///
    /// # Side Effects
    ///
    /// - Clears stream callbacks for all connections
    /// - Clears pending subscriptions for all connections
    /// - Removes all connection stream mappings
    pub async fn disconnect(&self) -> Result<(), WebsocketError> {
        let common = Arc::clone(&self.common);
        let connections = self.common.connection_pool.clone();
        let connection_streams = Arc::clone(&self.connection_streams);
        let terminal_events_emitter = self.stream_subscription_events.clone();
        match spawn(async move {
            let disconnect_result = common.disconnect().await;
            let mut terminal_events = Vec::new();
            for connection in connections {
                let pending = {
                    let mut conn_state = connection.state.lock().await;
                    // Also cover slots which had no active writer when the
                    // common transport shutdown began. No new subscription
                    // may enter after this drain and escape Disconnected.
                    conn_state.close_initiated = true;
                    let pending_ids = conn_state
                        .pending_stream_subscriptions
                        .keys()
                        .copied()
                        .collect::<Vec<_>>();
                    let pending = pending_ids
                        .into_iter()
                        .filter_map(|request_id| {
                            conn_state.remove_pending_stream_subscription(request_id, true)
                        })
                        .collect::<Vec<_>>();
                    conn_state.stream_callbacks.clear();
                    conn_state.pending_subscriptions.clear();
                    conn_state.confirmed_stream_keys.clear();
                    conn_state.unconfirmed_stream_reservations.clear();
                    conn_state.stream_request_id_generations.clear();
                    pending
                };
                for (pending, terminal_claimed) in pending {
                    let context = pending.context.clone();
                    if terminal_claimed {
                        terminal_events.push(context.clone());
                    }
                    let error = StreamSubscriptionError::Disconnected { context };
                    let _ = pending.completion.send(Err(error));
                }
            }
            connection_streams.lock().await.clear();
            for context in terminal_events {
                StreamSubscriptionLifecycle::emit_claimed(
                    &terminal_events_emitter,
                    context,
                    StreamSubscriptionOutcome::Disconnected,
                );
            }
            disconnect_result
        })
        .await
        {
            Ok(result) => result,
            Err(join_error) => {
                error!("Stream disconnect terminal owner failed: {}", join_error);
                Err(WebsocketError::ServerError(format!(
                    "stream disconnect terminal owner failed: {join_error}"
                )))
            }
        }
    }

    /// Permanently retires this stream runtime and all of its background
    /// transport ownership. Unlike [`Self::disconnect`], this operation is not
    /// reversible: reconnect/renewal loops, I/O actors, writers, and installed
    /// handlers are all terminated before it returns.
    #[cfg(feature = "derivatives_trading_usds_futures")]
    pub(crate) async fn terminal_shutdown(self: &Arc<Self>) -> Result<(), WebsocketError> {
        let owner = Arc::clone(self);
        match spawn(async move {
            owner.common.begin_terminal_shutdown();
            let graceful_result = owner.disconnect().await;
            owner.common.finish_terminal_shutdown().await;
            // Terminal shutdown force-retires every writer/actor after the
            // graceful attempt. A close-handshake timeout is therefore a
            // diagnostic, not failure to retire the exact client. The only
            // error returned below is failure of this cleanup owner itself.
            if graceful_result.is_err() {
                debug!("Exact-scope graceful close failed; forced terminal cleanup completed");
            }
            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(join_error) => Err(WebsocketError::ServerError(format!(
                "stream terminal shutdown owner failed: {join_error}"
            ))),
        }
    }

    /// Checks if the WebSocket connection is currently active.
    ///
    /// # Returns
    ///
    /// `true` if the WebSocket connection is established, `false` otherwise.
    pub async fn is_connected(&self) -> bool {
        self.common.is_connected(None).await
    }

    /// Sends a ping to the WebSocket server to maintain the connection.
    ///
    /// This method delegates the ping operation to the underlying common WebSocket connection.
    /// It is typically used to keep the connection alive and check its status.
    ///
    /// # Side Effects
    ///
    /// Sends a ping request to the WebSocket server through the common connection.
    pub async fn ping_server(&self) {
        self.common.ping_server().await;
    }

    /// Subscribes to correlated JSON stream-subscription outcomes for
    /// diagnostics. Delivery is asynchronous and must not be used as an
    /// ordered ingress gate relative to raw frames. Configure a synchronous
    /// [`StreamSubscriptionObserver`] for that purpose. Events carry only
    /// redacted connection/session/scope/request context.
    pub fn subscribe_on_stream_subscription_events<F>(&self, callback: F) -> Subscription
    where
        F: FnMut(StreamSubscriptionEvent) + Send + 'static,
    {
        self.stream_subscription_events.subscribe(callback)
    }

    /// Allocates an unsigned JSON request identifier for stream control.
    #[must_use]
    pub fn next_stream_request_id(&self) -> u32 {
        self.next_stream_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribes to multiple WebSocket streams, handling connection and queuing logic.
    ///
    /// # Arguments
    ///
    /// * `streams` - A vector of stream names to subscribe to
    /// * `id` - An optional request identifier for the subscription
    /// * `url_path` - An optional URL path for the subscription
    ///
    /// # Behavior
    ///
    /// - Filters out streams already subscribed
    /// - Assigns streams to appropriate connections
    /// - Handles subscription for active connections
    /// - Queues subscriptions for inactive connections
    ///
    /// # Side Effects
    ///
    /// - Sends subscription payloads for active connections
    /// - Adds pending subscriptions for inactive connections
    pub async fn subscribe(
        self: Arc<Self>,
        streams: Vec<String>,
        id: Option<StreamId>,
        url_path: Option<&str>,
    ) {
        let streams: Vec<String> = {
            let map = self.connection_streams.lock().await;
            streams
                .into_iter()
                .filter(|s| {
                    let key = self.stream_key(s, url_path);
                    !map.contains_key(&key)
                })
                .collect()
        };

        if streams.is_empty() {
            return;
        }

        let connection_streams = self.handle_stream_assignment(streams, url_path).await;

        for (conn, assigned_streams) in connection_streams {
            if !self.common.is_connected(Some(&conn)).await {
                info!(
                    "Connection {} is not ready; queuing {} subscription(s) for scope {}",
                    conn.id,
                    assigned_streams.len(),
                    redacted_path_scope(url_path)
                );

                let mut conn_state = conn.state.lock().await;
                conn_state
                    .pending_subscriptions
                    .extend(assigned_streams.iter().cloned());

                continue;
            }

            self.send_subscription_payload(&conn, &assigned_streams, id.clone());
        }
    }

    /// Subscribes one stream and resolves only after an ACK, a correlated
    /// reject, or a local timeout for the exact physical session used to send
    /// the request. No stream parameter is included in the result or error.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`StreamSubscriptionError`] when assignment,
    /// dispatch, acknowledgement, or session correlation fails.
    pub async fn subscribe_one_confirmed(
        self: Arc<Self>,
        stream: String,
        request_id: u32,
        ack_timeout: Duration,
        url_path: Option<&str>,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        let key = self.stream_key(&stream, url_path);
        let (connection, reservation_token) = self
            .reserve_one_stream_assignment(&key, request_id, url_path)
            .await?;
        let lifecycle = Arc::new(StreamSubscriptionLifecycle::new());

        let mut ownership_guard = UnconfirmedAssignmentGuard {
            connection_streams: Arc::clone(&self.connection_streams),
            connection: connection.clone(),
            stream_keys: vec![key.clone()],
            request_id,
            reservation_token,
            lifecycle: lifecycle.clone(),
            events: self.stream_subscription_events.clone(),
            armed: true,
        };

        let result = self
            .send_subscription_payload_confirmed(
                &connection,
                std::slice::from_ref(&stream),
                request_id,
                ack_timeout,
                Some(reservation_token),
                lifecycle.clone(),
            )
            .await;

        let ack = match result {
            Ok(ack) => ack,
            Err(error) => {
                self.release_stream_assignment(&key, &stream, &connection, reservation_token)
                    .await;
                ownership_guard.disarm();
                return Err(error);
            }
        };

        let mut connection_streams = self.connection_streams.lock().await;
        let owns_assignment = connection_streams
            .get(&key)
            .is_some_and(|assigned| Arc::ptr_eq(assigned, &connection));
        let mut state = connection.state.lock().await;
        let owns_reservation = state
            .unconfirmed_stream_reservations
            .get(&key)
            .is_some_and(|current| *current == reservation_token);
        let current_generation = state.writer_session_generation;
        if !owns_assignment
            || !owns_reservation
            || current_generation != Some(ack.context.session_generation)
        {
            if owns_assignment && owns_reservation {
                state.stream_callbacks.remove(&key);
                state.confirmed_stream_keys.remove(&key);
                state.unconfirmed_stream_reservations.remove(&key);
                connection_streams.remove(&key);
            }
            ownership_guard.disarm();
            drop(state);
            drop(connection_streams);
            lifecycle.emit_terminal(
                &self.stream_subscription_events,
                ack.context.clone(),
                StreamSubscriptionOutcome::SessionReplaced,
            );
            return Err(StreamSubscriptionError::SessionReplaced {
                context: ack.context,
            });
        }
        state.unconfirmed_stream_reservations.remove(&key);
        state.confirmed_stream_keys.insert(key);
        ownership_guard.disarm();
        drop(state);
        drop(connection_streams);
        lifecycle.emit_terminal(
            &self.stream_subscription_events,
            ack.context.clone(),
            StreamSubscriptionOutcome::Acknowledged,
        );
        Ok(ack)
    }

    /// Sends a confirmed JSON `SUBSCRIBE` to one exact routed `/public` or
    /// `/market` physical session.
    ///
    /// The caller owns the desired topic set, pacing, connection budget, and
    /// reconnect replay. This method never adds topics to SDK stream ownership
    /// and never resubscribes them automatically. Topics are treated as a set:
    /// they are sorted lexicographically and duplicates are removed before the
    /// payload is encoded.
    ///
    /// `request_id` is always encoded as the caller-supplied unsigned JSON
    /// integer. The caller must allocate monotonically increasing identifiers;
    /// the SDK does not replace it with a random or generated value.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`StreamSubscriptionError`] when the target is stale,
    /// the request is invalid, cannot be dispatched, is rejected, times out,
    /// or loses its exact physical session before acknowledgement.
    #[cfg(feature = "derivatives_trading_usds_futures")]
    pub(crate) async fn subscribe_routed_confirmed(
        &self,
        target: RoutedStreamTarget,
        topics: Vec<String>,
        request_id: u32,
        ack_timeout: Duration,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        self.routed_stream_control_confirmed(
            target,
            topics,
            request_id,
            ack_timeout,
            ConfirmedStreamControlMethod::Subscribe,
        )
        .await
    }

    /// Sends a confirmed JSON `UNSUBSCRIBE` to one exact routed `/public` or
    /// `/market` physical session.
    ///
    /// This is a wire-level control primitive only. It does not mutate SDK
    /// stream ownership, so the caller remains the single owner of the desired
    /// set across initial connect and reconnect.
    ///
    /// `request_id` is always encoded as the caller-supplied unsigned JSON
    /// integer. The caller must allocate monotonically increasing identifiers.
    /// Topics are sorted lexicographically and deduplicated before encoding.
    ///
    /// # Errors
    ///
    /// Returns a redacted [`StreamSubscriptionError`] under the same exact
    /// session and terminal-outcome rules as
    /// [`Self::subscribe_routed_confirmed`].
    #[cfg(feature = "derivatives_trading_usds_futures")]
    pub(crate) async fn unsubscribe_routed_confirmed(
        &self,
        target: RoutedStreamTarget,
        topics: Vec<String>,
        request_id: u32,
        ack_timeout: Duration,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        self.routed_stream_control_confirmed(
            target,
            topics,
            request_id,
            ack_timeout,
            ConfirmedStreamControlMethod::Unsubscribe,
        )
        .await
    }

    #[cfg(feature = "derivatives_trading_usds_futures")]
    async fn routed_stream_control_confirmed(
        &self,
        target: RoutedStreamTarget,
        mut topics: Vec<String>,
        request_id: u32,
        ack_timeout: Duration,
        control: ConfirmedStreamControlMethod,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        let lifecycle = Arc::new(StreamSubscriptionLifecycle::new());
        let context = target.subscription_context(request_id);
        lifecycle.set_context(context.clone());

        if topics.is_empty() || topics.iter().any(String::is_empty) {
            lifecycle.emit_terminal(
                &self.stream_subscription_events,
                context.clone(),
                StreamSubscriptionOutcome::ProtocolError,
            );
            return Err(StreamSubscriptionError::Protocol { context });
        }
        topics.sort_unstable();
        topics.dedup();

        let Some(connection) = self
            .common
            .connection_pool
            .iter()
            .find(|connection| connection.id == target.connection_id)
            .cloned()
        else {
            lifecycle.emit_terminal(
                &self.stream_subscription_events,
                context.clone(),
                StreamSubscriptionOutcome::SessionReplaced,
            );
            return Err(StreamSubscriptionError::SessionReplaced { context });
        };

        let mut cancellation_guard = RoutedStreamControlGuard {
            connection: connection.clone(),
            request_id,
            lifecycle: lifecycle.clone(),
            events: self.stream_subscription_events.clone(),
            armed: true,
        };
        let result = self
            .send_confirmed_stream_control(
                &connection,
                ConfirmedStreamControl {
                    method: control,
                    params: &topics,
                    ownership: ConfirmedStreamControlOwnership::CallerManaged { target },
                },
                request_id,
                ack_timeout,
                lifecycle,
            )
            .await;
        cancellation_guard.disarm();
        result
    }

    async fn reserve_one_stream_assignment(
        &self,
        key: &str,
        request_id: u32,
        url_path: Option<&str>,
    ) -> Result<(Arc<WebsocketConnection>, u64), StreamSubscriptionError> {
        let connection = self
            .common
            .get_connection(true, url_path)
            .await
            .map_err(|_| StreamSubscriptionError::NoConnection { request_id })?;
        let reservation_token = self
            .next_stream_reservation_token
            .fetch_add(1, Ordering::Relaxed);

        let mut assignments = self.connection_streams.lock().await;
        if assignments.contains_key(key) {
            return Err(StreamSubscriptionError::AlreadyDesired { request_id });
        }

        let mut state = connection.state.lock().await;
        state.stream_callbacks.entry(key.to_string()).or_default();
        state
            .unconfirmed_stream_reservations
            .insert(key.to_string(), reservation_token);
        assignments.insert(key.to_string(), connection.clone());
        drop(state);
        drop(assignments);
        Ok((connection, reservation_token))
    }

    async fn release_stream_assignment(
        &self,
        key: &str,
        stream: &str,
        connection: &Arc<WebsocketConnection>,
        reservation_token: u64,
    ) {
        let mut connection_streams = self.connection_streams.lock().await;
        if !connection_streams
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(current, connection))
        {
            return;
        }

        let mut state = connection.state.lock().await;
        let owns_reservation = state
            .unconfirmed_stream_reservations
            .get(key)
            .is_some_and(|current| *current == reservation_token);
        if !owns_reservation {
            return;
        }
        state.stream_callbacks.remove(key);
        state
            .pending_subscriptions
            .retain(|pending| pending != stream);
        state.confirmed_stream_keys.remove(key);
        state.unconfirmed_stream_reservations.remove(key);
        connection_streams.remove(key);
    }

    async fn send_subscription_payload_confirmed(
        &self,
        connection: &Arc<WebsocketConnection>,
        streams: &[String],
        request_id: u32,
        ack_timeout: Duration,
        unconfirmed_reservation_token: Option<u64>,
        lifecycle: Arc<StreamSubscriptionLifecycle>,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        self.send_confirmed_stream_control(
            connection,
            ConfirmedStreamControl {
                method: ConfirmedStreamControlMethod::Subscribe,
                params: streams,
                ownership: ConfirmedStreamControlOwnership::SdkManaged {
                    unconfirmed_reservation_token,
                },
            },
            request_id,
            ack_timeout,
            lifecycle,
        )
        .await
    }

    async fn send_confirmed_stream_control(
        &self,
        connection: &Arc<WebsocketConnection>,
        control: ConfirmedStreamControl<'_>,
        request_id: u32,
        ack_timeout: Duration,
        lifecycle: Arc<StreamSubscriptionLifecycle>,
    ) -> Result<StreamSubscriptionAck, StreamSubscriptionError> {
        let mut state = connection.state.lock().await;
        let context = control.context(connection, &state, request_id);
        lifecycle.set_context(context.clone());
        if !control.exact_session_is_current(connection, &state) {
            drop(state);
            if control.emit_pre_dispatch_terminal() {
                lifecycle.emit_terminal(
                    &self.stream_subscription_events,
                    context.clone(),
                    StreamSubscriptionOutcome::SessionReplaced,
                );
            }
            return Err(StreamSubscriptionError::SessionReplaced { context });
        }

        let payload = json!({
            "method": control.method.method(),
            "params": control.params,
            "id": request_id,
        });
        let Ok(message) = serde_json::to_string(&payload) else {
            drop(state);
            if control.emit_pre_dispatch_terminal() {
                lifecycle.emit_terminal(
                    &self.stream_subscription_events,
                    context.clone(),
                    StreamSubscriptionOutcome::ProtocolError,
                );
            }
            return Err(StreamSubscriptionError::Protocol { context });
        };

        #[cfg(feature = "derivatives_trading_usds_futures")]
        if matches!(
            control.pending_ownership(),
            PendingStreamSubscriptionOwnership::CallerManaged
        ) {
            let stale_caller_managed = state
                .pending_stream_subscriptions
                .get(&request_id)
                .is_some_and(|pending| {
                    pending.ownership == PendingStreamSubscriptionOwnership::CallerManaged
                        && pending.context.session_generation != context.session_generation
                });
            if stale_caller_managed {
                let (pending, terminal_claimed) = state
                    .remove_pending_stream_subscription(request_id, true)
                    .expect("matched stale caller-managed request must still be pending");
                let stale_context = pending.context.clone();
                let _ = pending
                    .completion
                    .send(Err(StreamSubscriptionError::SessionReplaced {
                        context: stale_context.clone(),
                    }));
                if terminal_claimed {
                    StreamSubscriptionLifecycle::emit_claimed(
                        &self.stream_subscription_events,
                        stale_context,
                        StreamSubscriptionOutcome::SessionReplaced,
                    );
                }
            }
        }

        if state.pending_stream_subscriptions.contains_key(&request_id)
            || state
                .stream_request_id_generations
                .get(&request_id)
                .is_some_and(|used_generation| *used_generation == context.session_generation)
        {
            drop(state);
            if control.emit_pre_dispatch_terminal() {
                lifecycle.emit_terminal(
                    &self.stream_subscription_events,
                    context.clone(),
                    StreamSubscriptionOutcome::ProtocolError,
                );
            }
            return Err(StreamSubscriptionError::DuplicateRequestId { context });
        }

        let (stream_keys, was_confirmed, unconfirmed_reservation_token, parameter_label) =
            match &control.ownership {
                ConfirmedStreamControlOwnership::SdkManaged {
                    unconfirmed_reservation_token,
                } => {
                    let stream_keys = control
                        .params
                        .iter()
                        .map(|stream| self.stream_key(stream, state.url_path.as_deref()))
                        .collect::<Vec<_>>();
                    let was_confirmed = stream_keys
                        .iter()
                        .all(|key| state.confirmed_stream_keys.contains(key));
                    let owns_unconfirmed_reservation =
                        unconfirmed_reservation_token.is_some_and(|token| {
                            stream_keys.iter().all(|key| {
                                state
                                    .unconfirmed_stream_reservations
                                    .get(key)
                                    .is_some_and(|current| *current == token)
                            })
                        });
                    if !was_confirmed && !owns_unconfirmed_reservation {
                        drop(state);
                        return Err(StreamSubscriptionError::SessionReplaced { context });
                    }
                    (
                        stream_keys,
                        was_confirmed,
                        *unconfirmed_reservation_token,
                        "stream(s)",
                    )
                }
                #[cfg(feature = "derivatives_trading_usds_futures")]
                ConfirmedStreamControlOwnership::CallerManaged { .. } => {
                    (Vec::new(), true, None, "topic(s)")
                }
            };

        let token = self
            .next_stream_pending_token
            .fetch_add(1, Ordering::Relaxed);
        let (completion, receiver) = oneshot::channel();
        state.pending_stream_subscriptions.insert(
            request_id,
            PendingStreamSubscription {
                token,
                context: context.clone(),
                #[cfg(feature = "derivatives_trading_usds_futures")]
                ownership: control.pending_ownership(),
                stream_keys: stream_keys.clone(),
                was_confirmed,
                unconfirmed_reservation_token,
                lifecycle: lifecycle.clone(),
                completion,
            },
        );
        state
            .stream_request_id_generations
            .insert(request_id, context.session_generation);
        lifecycle.emit_dispatched(&self.stream_subscription_events, &context);
        let writer = state.ws_write_tx.clone().filter(|_| {
            state.writer_session_generation == Some(context.session_generation)
                && !state.reconnection_pending
                && !state.close_initiated
        });
        let dispatch_failed =
            writer.is_none_or(|writer| writer.send(Message::Text(message.into())).is_err());

        if dispatch_failed {
            let (pending, terminal_claimed) = state
                .remove_pending_stream_subscription(request_id, true)
                .expect("just-inserted confirmed control request must still be pending");
            if state.stream_request_id_generations.get(&request_id)
                == Some(&context.session_generation)
            {
                state.stream_request_id_generations.remove(&request_id);
            }
            drop(state);
            finish_stream_terminal(
                Arc::clone(&self.connection_streams),
                connection.clone(),
                pending.stream_keys,
                pending.unconfirmed_reservation_token,
                self.stream_subscription_events.clone(),
                context.clone(),
                StreamSubscriptionOutcome::DispatchFailed,
                terminal_claimed,
            )
            .await;
            return Err(StreamSubscriptionError::NotConnected { context });
        }
        let path_scope = state.url_path.clone();
        drop(state);

        info!(
            "{} {} {} on connection {} generation {} for scope {} with request id {}",
            control.method.method(),
            control.params.len(),
            parameter_label,
            connection.id,
            context.session_generation,
            redacted_path_scope(path_scope.as_deref()),
            request_id
        );

        let timeout_connection = Arc::clone(connection);
        let timeout_assignments = Arc::clone(&self.connection_streams);
        let timeout_context = context.clone();
        let timeout_events = self.stream_subscription_events.clone();
        spawn(async move {
            sleep(ack_timeout).await;
            let timed_out = {
                let mut state = timeout_connection.state.lock().await;
                let matches_token = state
                    .pending_stream_subscriptions
                    .get(&request_id)
                    .is_some_and(|pending| pending.token == token);
                if matches_token {
                    state.remove_pending_stream_subscription(request_id, true)
                } else {
                    None
                }
            };
            if let Some((pending, terminal_claimed)) = timed_out {
                let error = StreamSubscriptionError::Timeout {
                    context: timeout_context.clone(),
                };
                let _ = pending.completion.send(Err(error));
                finish_stream_terminal(
                    timeout_assignments,
                    timeout_connection,
                    pending.stream_keys,
                    pending.unconfirmed_reservation_token,
                    timeout_events,
                    timeout_context,
                    StreamSubscriptionOutcome::TimedOut,
                    terminal_claimed,
                )
                .await;
            }
        });

        match receiver.await {
            Ok(result) => result,
            Err(_) => {
                lifecycle.emit_terminal(
                    &self.stream_subscription_events,
                    context.clone(),
                    StreamSubscriptionOutcome::ProtocolError,
                );
                Err(StreamSubscriptionError::ResponseChannelClosed { context })
            }
        }
    }

    /// Unsubscribes from specified WebSocket streams.
    ///
    /// # Arguments
    ///
    /// * `streams` - A vector of stream names to unsubscribe from
    /// * `id` - An optional request identifier for the unsubscription
    /// * `url_path` - An optional URL path for the unsubscription
    ///
    /// # Behavior
    ///
    /// - Validates the request identifier or generates a random one
    /// - Checks for subscribed streams and active callbacks
    /// - Sends an unsubscribe payload when the associated connection is active
    /// - Removes stream ownership from local state even when the connection is inactive
    ///
    /// # Side Effects
    ///
    /// - Sends unsubscribe request to WebSocket server
    /// - Removes stream tracking from internal state
    ///
    /// # Async
    ///
    /// This method is asynchronous and requires `.await` when called
    ///
    /// # Panics
    ///
    /// This method may panic if the request identifier is not valid.
    ///
    pub async fn unsubscribe(
        &self,
        streams: Vec<String>,
        id: Option<StreamId>,
        url_path: Option<&str>,
    ) {
        let request_id = normalize_stream_id(
            id.clone(),
            self.stream_id_is_strictly_number.load(Ordering::Relaxed),
        );

        for stream in streams {
            let key = self.stream_key(&stream, url_path);
            let (conn, was_connected) = {
                let mut connection_streams = self.connection_streams.lock().await;
                let Some(conn) = connection_streams.get(&key).cloned() else {
                    warn!("Requested subscription was not present");
                    continue;
                };
                let mut conn_state = conn.state.lock().await;
                let has_callbacks = conn_state
                    .stream_callbacks
                    .get(&key)
                    .is_some_and(|v| !v.is_empty());

                if has_callbacks {
                    continue;
                }

                conn_state.confirmed_stream_keys.remove(&key);
                conn_state.unconfirmed_stream_reservations.remove(&key);
                let was_connected = conn_state.ws_write_tx.is_some()
                    && !conn_state.reconnection_pending
                    && !conn_state.close_initiated;
                conn_state.stream_callbacks.remove(&key);
                conn_state
                    .pending_subscriptions
                    .retain(|pending| pending != &stream);
                if connection_streams
                    .get(&key)
                    .is_some_and(|current| Arc::ptr_eq(current, &conn))
                {
                    connection_streams.remove(&key);
                }
                drop(conn_state);
                drop(connection_streams);
                (conn, was_connected)
            };

            if was_connected {
                let payload = json!({
                    "method": "UNSUBSCRIBE",
                    "params": [stream.clone()],
                    "id": request_id,
                });

                info!(
                    "UNSUBSCRIBE one stream on connection {} for scope {}",
                    conn.id,
                    redacted_path_scope(url_path)
                );

                let common = Arc::clone(&self.common);
                let conn_clone = Arc::clone(&conn);
                let msg = serde_json::to_string(&payload).unwrap();
                spawn(async move {
                    let _ = common
                        .send(msg, None, false, Duration::ZERO, Some(conn_clone))
                        .await;
                });
            } else {
                warn!(
                    "Subscription is not associated with an active connection; releasing local state"
                );
            }
        }
    }

    /// Checks if a specific stream is currently subscribed.
    ///
    /// # Arguments
    ///
    /// * `stream` - The stream identifier to check for subscription status
    ///
    /// # Returns
    ///
    /// `true` if the stream is subscribed, `false` otherwise
    ///
    /// # Async
    ///
    /// This method is asynchronous and requires `.await` when called
    pub async fn is_subscribed(&self, stream: &str) -> bool {
        let map = self.connection_streams.lock().await;

        if map.contains_key(stream) {
            return true;
        }

        let suffix = format!("::{}", stream);
        map.keys().any(|k| k.ends_with(&suffix))
    }

    /// Generates a unique key for a stream based on its name and optional URL path.
    ///
    /// # Arguments
    ///
    /// * `stream` - The name of the stream
    /// * `url_path` - An optional URL path associated with the stream
    ///
    /// # Returns
    ///
    /// A `String` representing the unique key for the stream
    ///
    fn stream_key(&self, stream: &str, url_path: Option<&str>) -> String {
        match url_path {
            Some(p) if !p.is_empty() => format!("{p}::{stream}"),
            _ => stream.to_string(),
        }
    }

    /// Prepares a WebSocket URL for streaming with optional stream names and time unit configuration.
    ///
    /// # Arguments
    ///
    /// * `streams` - A slice of stream names to be included in the URL
    /// * `url_path` - An optional path to append to the base WebSocket URL
    ///
    /// # Returns
    ///
    /// A fully constructed WebSocket URL with optional stream and time unit parameters
    ///
    /// # Notes
    ///
    /// - If no time unit is specified, the base URL is returned
    /// - Validates and appends the time unit parameter if provided and valid
    /// - Handles URL parameter separator based on existing query parameters
    fn prepare_url(&self, streams: &[String], url_path: Option<&str>) -> String {
        if url_path == Some("private") {
            // Binance documents JSON SUBSCRIBE on the dedicated private route,
            // but does not specify the extended listenKey+events JSON item
            // shape. Connect to the query-free ws endpoint and keep using the
            // generated SDK's established single-listen-key parameter. The
            // official Java and Go connectors use the query-free /private/stream
            // root for this JSON control path. Never place the private parameter
            // into a URL.
            return format!(
                "{}/private/stream",
                self.configuration.ws_url.as_deref().unwrap_or("")
            );
        }

        if streams.is_empty() && matches!(url_path, Some("public" | "market")) {
            // A caller using confirmed routed JSON controls owns the complete
            // desired set. Open the documented combined-stream route without
            // URL subscriptions, preserving combined response envelopes while
            // avoiding an empty `?streams=` subscription or double initial
            // ownership. Binance's live-control protocol then supplies the
            // first SUBSCRIBE explicitly.
            return format!(
                "{}/{}/stream",
                self.configuration.ws_url.as_deref().unwrap_or(""),
                url_path.expect("matched routed path")
            );
        }

        let mut url = format!(
            "{}/stream?streams={}",
            match url_path {
                Some(path) => format!(
                    "{}/{}",
                    self.configuration.ws_url.as_deref().unwrap_or(""),
                    path
                ),
                None => self
                    .configuration
                    .ws_url
                    .as_deref()
                    .unwrap_or("")
                    .to_string(),
            },
            streams.join("/")
        );

        let time_unit = match &self.configuration.time_unit {
            Some(u) => u.to_string(),
            None => return url,
        };

        match validate_time_unit(&time_unit) {
            Ok(Some(validated)) => {
                let sep = if url.contains('?') { '&' } else { '?' };
                url.push(sep);
                url.push_str("timeUnit=");
                url.push_str(validated);
            }
            Ok(None) => {}
            Err(e) => {
                error!("Invalid time unit provided: {:?}", e);
            }
        }

        url
    }

    /// Handles stream assignment by finding or creating WebSocket connections for a list of streams.
    ///
    /// This method attempts to assign streams to existing WebSocket connections or creates new
    /// connections if needed. It groups streams by their assigned connections and handles scenarios
    /// such as closed or pending reconnection connections.
    ///
    /// # Arguments
    ///
    /// * `streams` - A vector of stream names to be assigned
    /// * `url_path` - An optional URL path associated with the streams
    ///
    /// # Returns
    ///
    /// A vector of tuples containing WebSocket connections and their associated streams
    ///
    /// # Errors
    ///
    /// Returns an empty result if no connections can be established for the streams
    async fn handle_stream_assignment(
        &self,
        streams: Vec<String>,
        url_path: Option<&str>,
    ) -> Vec<(Arc<WebsocketConnection>, Vec<String>)> {
        let mut connection_streams: Vec<(String, Arc<WebsocketConnection>)> = Vec::new();

        for stream in streams {
            let key = self.stream_key(&stream, url_path);

            let mut conn_opt = {
                let map = self.connection_streams.lock().await;
                map.get(&key).cloned()
            };

            let need_new = if let Some(conn) = &conn_opt {
                let state = conn.state.lock().await;
                state.close_initiated || state.reconnection_pending
            } else {
                true
            };

            if need_new {
                let previous_connection = conn_opt.clone();
                match self.common.get_connection(true, url_path).await {
                    Ok(new_conn) => {
                        let mut map = self.connection_streams.lock().await;
                        let mapping_is_unchanged =
                            match (map.get(&key), previous_connection.as_ref()) {
                                (None, None) => true,
                                (Some(current), Some(previous)) => Arc::ptr_eq(current, previous),
                                _ => false,
                            };
                        if mapping_is_unchanged {
                            map.insert(key.clone(), new_conn.clone());
                            conn_opt = Some(new_conn);
                        } else {
                            conn_opt = map.get(&key).cloned();
                        }
                    }
                    Err(_) => {
                        warn!(
                            "No available WebSocket connection for {} scope",
                            redacted_path_scope(url_path)
                        );
                        continue;
                    }
                }
            }

            if let Some(conn) = conn_opt {
                {
                    let mut conn_state = conn.state.lock().await;
                    conn_state.stream_callbacks.entry(key.clone()).or_default();
                }
                connection_streams.push((stream, conn));
            }
        }

        let mut groups: Vec<(Arc<WebsocketConnection>, Vec<String>)> = Vec::new();
        for (stream, conn) in connection_streams {
            if let Some((_, vec)) = groups.iter_mut().find(|(c, _)| Arc::ptr_eq(c, &conn)) {
                vec.push(stream);
            } else {
                groups.push((conn, vec![stream]));
            }
        }

        groups
    }

    /// Sends a WebSocket subscription payload for the specified streams.
    ///
    /// # Arguments
    ///
    /// * `connection` - The WebSocket connection to send the subscription on
    /// * `streams` - A vector of stream names to subscribe to
    /// * `id` - An optional request ID for the subscription (will be randomly generated if not provided)
    ///
    /// # Remarks
    ///
    /// This method constructs a SUBSCRIBE payload, logs it, and sends it asynchronously using the WebSocket connection.
    /// If serialization fails, an error is logged and the method returns without sending.
    fn send_subscription_payload(
        &self,
        connection: &Arc<WebsocketConnection>,
        streams: &Vec<String>,
        id: Option<StreamId>,
    ) {
        let request_id = normalize_stream_id(
            id.clone(),
            self.stream_id_is_strictly_number.load(Ordering::Relaxed),
        );

        let payload = json!({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": request_id,
        });

        let path_scope = connection
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.url_path.clone());
        info!(
            "SUBSCRIBE {} stream(s) on connection {} for scope {}",
            streams.len(),
            connection.id,
            redacted_path_scope(path_scope.as_deref())
        );

        let common = Arc::clone(&self.common);
        let msg = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to serialize SUBSCRIBE payload: {}", e);
                return;
            }
        };
        let conn_clone = Arc::clone(connection);

        spawn(async move {
            let _ = common
                .send(msg, None, false, Duration::ZERO, Some(conn_clone))
                .await;
        });
    }
}

#[async_trait]
impl WebsocketHandler for WebsocketStreams {
    /// Handles the WebSocket connection opening by processing any pending subscriptions.
    ///
    /// This method is called when a WebSocket connection is established. It retrieves
    /// any pending stream subscriptions from the connection state and sends them
    /// immediately using the `send_subscription_payload` method.
    ///
    /// # Arguments
    ///
    /// * `_url` - The URL of the WebSocket connection (unused)
    /// * `connection` - The WebSocket connection that has just been opened
    ///
    /// # Remarks
    ///
    /// If there are any pending subscriptions, they are sent as a batch subscription
    /// payload. The method uses a lock to safely access and clear the pending subscriptions
    /// from the connection state.
    async fn on_open(&self, _url: String, connection: Arc<WebsocketConnection>) {
        let (path_scope, pending_subs, stale_pending, confirmed_streams) = {
            let mut conn_state = connection.state.lock().await;
            let session_generation = conn_state
                .writer_session_generation
                .unwrap_or_else(|| connection.session_generation.load(Ordering::Acquire));
            let stale_ids = conn_state
                .pending_stream_subscriptions
                .iter()
                .filter_map(|(request_id, pending)| {
                    (pending.context.session_generation != session_generation)
                        .then_some(*request_id)
                })
                .collect::<Vec<_>>();
            let stale_pending = stale_ids
                .into_iter()
                .filter_map(|request_id| {
                    conn_state.remove_pending_stream_subscription(request_id, true)
                })
                .collect::<Vec<_>>();
            (
                conn_state.url_path.clone(),
                take(&mut conn_state.pending_subscriptions)
                    .into_iter()
                    .collect::<Vec<_>>(),
                stale_pending,
                conn_state.confirmed_stream_keys.clone(),
            )
        };

        let stale_terminal_owners = stale_pending
            .into_iter()
            .map(|(pending, terminal_claimed)| {
                let error = StreamSubscriptionError::SessionReplaced {
                    context: pending.context.clone(),
                };
                let _ = pending.completion.send(Err(error));
                spawn_stream_terminal_owner(
                    Arc::clone(&self.connection_streams),
                    connection.clone(),
                    pending.stream_keys,
                    pending.unconfirmed_reservation_token,
                    self.stream_subscription_events.clone(),
                    pending.context,
                    StreamSubscriptionOutcome::SessionReplaced,
                    terminal_claimed,
                )
            })
            .collect::<Vec<_>>();
        for terminal_owner in stale_terminal_owners {
            if let Err(join_error) = terminal_owner.await {
                error!(
                    "Stale stream subscription terminal owner failed: {}",
                    join_error
                );
            }
        }

        if path_scope.as_deref() == Some("private") {
            let mut desired_subscriptions = {
                let connection_streams = self.connection_streams.lock().await;
                connection_streams
                    .iter()
                    .filter_map(|(key, assigned)| {
                        if Arc::ptr_eq(assigned, &connection) && confirmed_streams.contains(key) {
                            key.strip_prefix("private::")
                                .map(std::string::ToString::to_string)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            };
            desired_subscriptions.sort_unstable();
            desired_subscriptions.dedup();

            if !desired_subscriptions.is_empty() {
                let request_id = self.next_stream_request_id();
                if let Err(error) = self
                    .send_subscription_payload_confirmed(
                        &connection,
                        &desired_subscriptions,
                        request_id,
                        STREAM_SUBSCRIPTION_ACK_TIMEOUT,
                        None,
                        Arc::new(StreamSubscriptionLifecycle::new()),
                    )
                    .await
                {
                    warn!(
                        "Private stream resubscription failed on connection {} generation {} with request id {}: {}",
                        connection.id,
                        connection.session_generation.load(Ordering::Acquire),
                        request_id,
                        error
                    );
                }
            }
            return;
        }

        if !pending_subs.is_empty() {
            info!("Processing queued subscriptions for connection");
            self.send_subscription_payload(&connection, &pending_subs, None);
        }
    }

    /// Handles incoming WebSocket stream messages by parsing the JSON payload and invoking registered stream callbacks.
    ///
    /// This method processes WebSocket messages with a specific structure, extracting the stream name and data.
    /// It retrieves and executes any registered callbacks associated with the stream name.
    ///
    /// # Arguments
    ///
    /// * `data` - The raw WebSocket message as a JSON-formatted string
    /// * `connection` - The WebSocket connection through which the message was received
    ///
    /// # Behavior
    ///
    /// - Parses the JSON message
    /// - Extracts the stream name and data payload
    /// - Looks up and invokes any registered callbacks for the stream
    /// - Silently returns if message parsing or stream extraction fails
    async fn on_message(&self, data: String, connection: Arc<WebsocketConnection>) {
        let msg: Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(err) => {
                error!("Failed to parse WebSocket stream message: {}", err);
                return;
            }
        };

        let (stream_name, payload) = match (
            msg.get("stream").and_then(Value::as_str),
            msg.get("data").cloned(),
        ) {
            (Some(name), Some(data)) => (name.to_string(), data),
            _ => return,
        };

        let callbacks = {
            let conn_state = connection.state.lock().await;
            let key = self.stream_key(&stream_name, conn_state.url_path.as_deref());
            conn_state
                .stream_callbacks
                .get(&key)
                .cloned()
                .unwrap_or_else(Vec::new)
        };

        for callback in callbacks {
            callback(&payload);
        }
    }

    async fn on_message_with_session(
        &self,
        data: String,
        connection: Arc<WebsocketConnection>,
        session_generation: u64,
    ) {
        let parsed: Option<Value> = serde_json::from_str(&data).ok();
        let request_id = parsed
            .as_ref()
            .and_then(|message| message.get("id"))
            .and_then(Value::as_u64)
            .and_then(|id| u32::try_from(id).ok());

        if let (Some(message), Some(request_id)) = (parsed.as_ref(), request_id) {
            let is_ack = message.as_object().is_some_and(|object| {
                object.len() == 2
                    && object.contains_key("id")
                    && object.get("result").is_some_and(Value::is_null)
            });
            let reject_code = message.as_object().and_then(|object| {
                let has_exact_keys = (object.len() == 2 || object.len() == 3)
                    && object.contains_key("id")
                    && object.contains_key("code")
                    && object
                        .keys()
                        .all(|key| matches!(key.as_str(), "id" | "code" | "msg"))
                    && object.get("msg").is_none_or(Value::is_string);
                has_exact_keys
                    .then(|| object.get("code").and_then(Value::as_i64))
                    .flatten()
            });
            let correlated = {
                let mut state = connection.state.lock().await;
                if let Some(pending) = state.pending_stream_subscriptions.get(&request_id) {
                    if pending.context.session_generation != session_generation
                        || state.writer_session_generation != Some(session_generation)
                    {
                        None
                    } else {
                        let receiver_closed = pending.completion.is_closed();
                        let release_unconfirmed = pending.unconfirmed_reservation_token.is_some()
                            && (receiver_closed || !is_ack);
                        let (result, mut outcome) = if let Some(code) = reject_code {
                            (
                                Err(StreamSubscriptionError::Rejected {
                                    context: pending.context.clone(),
                                    code,
                                }),
                                StreamSubscriptionOutcome::Rejected { code },
                            )
                        } else if is_ack {
                            (
                                Ok(StreamSubscriptionAck {
                                    context: pending.context.clone(),
                                }),
                                StreamSubscriptionOutcome::Acknowledged,
                            )
                        } else {
                            (
                                Err(StreamSubscriptionError::Protocol {
                                    context: pending.context.clone(),
                                }),
                                StreamSubscriptionOutcome::ProtocolError,
                            )
                        };
                        if pending.receiver_cancellation_wins(receiver_closed) {
                            outcome = StreamSubscriptionOutcome::Cancelled;
                        }
                        let emit_event = !is_ack || pending.was_confirmed || receiver_closed;
                        state
                            .remove_pending_stream_subscription(request_id, emit_event)
                            .map(|(pending, terminal_claimed)| {
                                (
                                    pending,
                                    result,
                                    outcome,
                                    terminal_claimed,
                                    release_unconfirmed,
                                )
                            })
                    }
                } else {
                    None
                }
            };

            if let Some((pending, result, outcome, terminal_claimed, release_unconfirmed)) =
                correlated
            {
                let event_context = pending.context.clone();
                let _ = pending.completion.send(result);
                finish_stream_terminal(
                    Arc::clone(&self.connection_streams),
                    connection,
                    pending.stream_keys,
                    release_unconfirmed
                        .then_some(pending.unconfirmed_reservation_token)
                        .flatten(),
                    self.stream_subscription_events.clone(),
                    event_context,
                    outcome,
                    terminal_claimed,
                )
                .await;
                return;
            }
        }

        self.on_message(data, connection).await;
    }

    /// Retrieves the reconnection URL for a specific WebSocket connection by identifying all streams associated with that connection.
    ///
    /// # Arguments
    ///
    /// * `_default_url` - A default URL that can be used if no specific reconnection URL is determined
    /// * `connection` - The WebSocket connection for which to generate a reconnection URL
    ///
    /// # Returns
    ///
    /// A URL string that can be used to reconnect to the WebSocket, based on the streams associated with the given connection
    async fn get_reconnect_url(
        &self,
        _default_url: String,
        connection: Arc<WebsocketConnection>,
    ) -> String {
        let connection_streams = self.connection_streams.lock().await;
        let reconnect_streams = connection_streams
            .iter()
            .filter_map(|(key, conn_arc)| {
                if Arc::ptr_eq(conn_arc, &connection) {
                    let stream = match key.split_once("::") {
                        Some((_prefix, rest)) => rest.to_string(),
                        None => key.clone(),
                    };
                    Some(stream)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let url_path = {
            let st = connection.state.lock().await;
            st.url_path.as_deref().map(std::string::ToString::to_string)
        };

        self.prepare_url(&reconnect_streams, url_path.as_deref())
    }
}

pub struct WebsocketStream<T> {
    websocket_base: WebsocketBase,
    stream_or_id: String,
    url_path: Option<String>,
    callback: Mutex<Option<Arc<dyn Fn(&Value) + Send + Sync>>>,
    pub id: Option<StreamId>,
    _phantom: PhantomData<T>,
}

impl<T> WebsocketStream<T>
where
    T: DeserializeOwned + Send + 'static,
{
    /// Registers a callback function for a specific event on the WebSocket stream.
    ///
    /// This method currently only supports the "message" event. When a message is received,
    /// the provided callback function will be invoked with the deserialized payload.
    ///
    /// # Arguments
    ///
    /// * `event` - The event type to listen for (currently only "message" is supported)
    /// * `callback_fn` - A function that will be called with the deserialized message payload
    ///
    /// # Errors
    ///
    /// Logs an error if the payload cannot be deserialized into the expected type
    ///
    /// # Examples
    ///
    ///
    /// stream.on("message", |data: `MyType`| {
    ///     // Handle the deserialized message
    /// });
    async fn on<F>(&self, event: &str, callback_fn: F)
    where
        F: Fn(T) + Send + Sync + 'static,
    {
        if event != "message" {
            return;
        }

        let cb_wrapper: Arc<dyn Fn(&Value) + Send + Sync> =
            Arc::new(
                move |v: &Value| match serde_json::from_value::<T>(v.clone()) {
                    Ok(data) => callback_fn(data),
                    Err(e) => error!("Failed to deserialize stream payload: {:?}", e),
                },
            );

        {
            let mut guard = self.callback.lock().await;
            *guard = Some(cb_wrapper.clone());
        }

        match &self.websocket_base {
            WebsocketBase::WebsocketStreams(ws_streams) => {
                let key = ws_streams.stream_key(&self.stream_or_id, self.url_path.as_deref());
                let conn = {
                    let map = ws_streams.connection_streams.lock().await;
                    map.get(&key).cloned().expect("stream must be subscribed")
                };

                {
                    let mut conn_state = conn.state.lock().await;
                    let entry = conn_state.stream_callbacks.entry(key).or_default();

                    if !entry
                        .iter()
                        .any(|existing| Arc::ptr_eq(existing, &cb_wrapper))
                    {
                        entry.push(cb_wrapper);
                    }
                }
            }
            WebsocketBase::WebsocketApi(ws_api) => {
                let mut stream_callbacks = ws_api.stream_callbacks.lock().await;
                let entry = stream_callbacks
                    .entry(self.stream_or_id.clone())
                    .or_default();

                if !entry
                    .iter()
                    .any(|existing| Arc::ptr_eq(existing, &cb_wrapper))
                {
                    entry.push(cb_wrapper);
                }
            }
        }
    }

    /// Synchronously sets a message callback for the WebSocket stream on the current thread.
    ///
    /// # Arguments
    ///
    /// * `callback_fn` - A function that will be called with the deserialized message payload
    ///
    /// # Panics
    ///
    /// Panics if the thread runtime fails to be created or if the thread join fails
    ///
    /// # Examples
    ///
    ///
    /// let stream = `Arc::new(WebsocketStream::new())`;
    /// `stream.on_message(|data`: `MyType`| {
    ///     // Handle the deserialized message
    /// });
    ///
    pub fn on_message<F>(self: &Arc<Self>, callback_fn: F)
    where
        T: Send + Sync,
        F: Fn(T) + Send + Sync + 'static,
    {
        let handler: Arc<Self> = Arc::clone(self);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build Tokio runtime");

            rt.block_on(handler.on("message", callback_fn));
        })
        .join()
        .expect("on_message thread panicked");
    }

    /// Unsubscribes from the current WebSocket stream and removes the associated callback.
    ///
    /// This method performs the following actions:
    /// - Removes the current callback associated with the stream
    /// - Removes the callback from the connection's stream callbacks
    /// - Asynchronously unsubscribes from the stream using the WebSocket streams base
    ///
    /// # Panics
    ///
    /// Panics if the stream is not subscribed to
    ///
    /// # Notes
    /// - Stream subscriptions are released even when no typed callback was registered
    /// - Local subscription state is released before this method returns
    pub async fn unsubscribe(&self) {
        let maybe_cb = {
            let mut guard = self.callback.lock().await;
            guard.take()
        };

        match &self.websocket_base {
            WebsocketBase::WebsocketStreams(ws_streams) => {
                if let Some(cb) = maybe_cb {
                    let key = ws_streams.stream_key(&self.stream_or_id, self.url_path.as_deref());
                    let conn = {
                        let map = ws_streams.connection_streams.lock().await;
                        map.get(&key)
                            .cloned()
                            .expect("stream must have been subscribed")
                    };

                    {
                        let mut conn_state = conn.state.lock().await;
                        if let Some(list) = conn_state.stream_callbacks.get_mut(&key) {
                            list.retain(|existing| !Arc::ptr_eq(existing, &cb));
                        }
                    }
                }

                ws_streams
                    .unsubscribe(
                        vec![self.stream_or_id.clone()],
                        self.id.clone(),
                        self.url_path.as_deref(),
                    )
                    .await;
            }
            WebsocketBase::WebsocketApi(ws_api) => {
                if let Some(cb) = maybe_cb {
                    let mut stream_callbacks = ws_api.stream_callbacks.lock().await;
                    if let Some(list) = stream_callbacks.get_mut(&self.stream_or_id) {
                        list.retain(|existing| !Arc::ptr_eq(existing, &cb));
                    }
                }
            }
        }
    }
}

/// Creates a new WebSocket stream handler for the specified stream or ID.
/// This function subscribes to the stream if the WebSocket base is of type `WebsocketStreams`.
///
/// # Arguments
///
/// * `websocket_base` - The base WebSocket instance (either `WebsocketStreams` or `WebsocketApi`)
/// * `stream_or_id` - The stream name or identifier to subscribe to
/// * `id` - An optional request identifier for the subscription
/// * `url_path` - An optional URL path for the subscription
///
/// # Returns
///
/// A new `WebsocketStream` instance.
///
pub async fn create_stream_handler<T>(
    websocket_base: WebsocketBase,
    stream_or_id: String,
    id: Option<StreamId>,
    url_path: Option<String>,
) -> Arc<WebsocketStream<T>>
where
    T: DeserializeOwned + Send + 'static,
{
    match &websocket_base {
        WebsocketBase::WebsocketStreams(ws_streams) => {
            ws_streams
                .clone()
                .subscribe(vec![stream_or_id.clone()], id.clone(), url_path.as_deref())
                .await;
        }
        WebsocketBase::WebsocketApi(_) => {}
    }

    Arc::new(WebsocketStream {
        websocket_base,
        stream_or_id,
        url_path,
        id,
        callback: Mutex::new(None),
        _phantom: PhantomData,
    })
}

/// Creates a stream handler only after an exact numeric JSON SUBSCRIBE request
/// has been acknowledged for the physical connection session that sent it.
///
/// # Errors
///
/// Returns a redacted [`StreamSubscriptionError`] if the stream base is not a
/// connected stream transport or the exact request is not acknowledged.
pub async fn create_stream_handler_confirmed<T>(
    websocket_base: WebsocketBase,
    stream_or_id: String,
    request_id: u32,
    ack_timeout: Duration,
    url_path: Option<String>,
) -> Result<(Arc<WebsocketStream<T>>, StreamSubscriptionAck), StreamSubscriptionError>
where
    T: DeserializeOwned + Send + 'static,
{
    let ack = match &websocket_base {
        WebsocketBase::WebsocketStreams(ws_streams) => {
            ws_streams
                .clone()
                .subscribe_one_confirmed(
                    stream_or_id.clone(),
                    request_id,
                    ack_timeout,
                    url_path.as_deref(),
                )
                .await?
        }
        WebsocketBase::WebsocketApi(_) => {
            return Err(StreamSubscriptionError::NoConnection { request_id });
        }
    };

    Ok((
        Arc::new(WebsocketStream {
            websocket_base,
            stream_or_id,
            url_path,
            id: Some(StreamId::Number(request_id)),
            callback: Mutex::new(None),
            _phantom: PhantomData,
        }),
        ack,
    ))
}

#[cfg(test)]
mod tests {
    use crate::TOKIO_SHARED_RT;
    use crate::common::utils::{SignatureGenerator, build_user_agent};
    use crate::common::websocket::{
        MAX_CONN_DURATION, PendingRequest, ReconnectEntry, RenewalEntry,
        SendWebsocketMessageResult, Subscription, WebsocketApi, WebsocketBase, WebsocketCommon,
        WebsocketConnection, WebsocketEvent, WebsocketEventEmitter, WebsocketHandler,
        WebsocketMessageSendOptions, WebsocketMode, WebsocketSessionLogonReq, WebsocketStream,
        WebsocketStreams, create_stream_handler, redacted_websocket_url,
    };
    use crate::config::{
        ConfigurationWebsocketApi, ConfigurationWebsocketStreams, PrivateKey, RawFrameKind,
        RawFrameObserver, StreamSubscriptionObserver, WebsocketLifecycleEvent,
    };
    use crate::errors::{
        StreamSubscriptionError, WebsocketConnectionFailureReason, WebsocketError,
    };
    #[cfg(feature = "derivatives_trading_usds_futures")]
    use crate::models::{RoutedStreamScope, RoutedStreamTarget};
    use crate::models::{
        StreamId, StreamSubscriptionEvent, StreamSubscriptionOutcome, StreamSubscriptionScope,
        TimeUnit,
    };
    use async_trait::async_trait;
    use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
    use futures::{SinkExt, StreamExt};
    use http::header::USER_AGENT;
    use regex::Regex;
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, HashSet};
    use std::io::{Read, Write};
    use std::marker::PhantomData;
    use std::net::SocketAddr;
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::net::TcpListener;
    use tokio::sync::{
        Mutex,
        mpsc::{Receiver, unbounded_channel},
        oneshot, watch,
    };
    use tokio::time::{Duration, advance, pause, resume, sleep, timeout};
    use tokio_tungstenite::{
        accept_async, accept_hdr_async, tungstenite,
        tungstenite::{
            Message,
            protocol::{CloseFrame, frame::coding::CloseCode},
        },
    };
    use tungstenite::handshake::server::Request;

    /// RAII guard that aborts a spawned task when dropped.
    ///
    /// Used by tests to bound the lifetime of mock listener / spawned helper tasks
    /// to the scope of the test, so they don't leak into `TOKIO_SHARED_RT` and
    /// add nondeterministic background load to subsequent tests.
    struct AbortOnDrop(tokio::task::JoinHandle<()>);

    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    struct RawOrderHandler {
        raw_seen: Arc<AtomicBool>,
        order: Arc<StdMutex<Vec<&'static str>>>,
        messages: Arc<StdMutex<Vec<String>>>,
    }

    #[async_trait]
    impl WebsocketHandler for RawOrderHandler {
        async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {}

        async fn on_message(&self, data: String, _connection: Arc<WebsocketConnection>) {
            assert!(
                self.raw_seen.load(Ordering::SeqCst),
                "raw observer must finish before the decode handler starts"
            );
            self.order.lock().unwrap().push("handler");
            self.messages.lock().unwrap().push(data);
        }

        async fn get_reconnect_url(
            &self,
            default_url: String,
            _connection: Arc<WebsocketConnection>,
        ) -> String {
            default_url
        }
    }

    #[test]
    fn raw_observer_runs_before_text_decode_and_binary_decompression() {
        TOKIO_SHARED_RT.block_on(async {
            let raw_seen = Arc::new(AtomicBool::new(false));
            let order = Arc::new(StdMutex::new(Vec::new()));
            let payloads = Arc::new(StdMutex::new(Vec::<Vec<u8>>::new()));
            let messages = Arc::new(StdMutex::new(Vec::<String>::new()));

            let observer = {
                let raw_seen = Arc::clone(&raw_seen);
                let order = Arc::clone(&order);
                let payloads = Arc::clone(&payloads);
                RawFrameObserver::new(move |connection_id, payload| {
                    assert_eq!(connection_id, "raw-order");
                    payloads.lock().unwrap().push(payload.to_vec());
                    order.lock().unwrap().push("raw");
                    raw_seen.store(true, Ordering::SeqCst);
                })
            };
            let connection = WebsocketConnection::new("raw-order");
            connection
                .set_handler(Arc::new(RawOrderHandler {
                    raw_seen: Arc::clone(&raw_seen),
                    order: Arc::clone(&order),
                    messages: Arc::clone(&messages),
                }))
                .await;
            let common = WebsocketCommon::new_with_raw_frame_observer(
                vec![Arc::clone(&connection)],
                WebsocketMode::Single,
                0,
                None,
                None,
                Some(observer),
            );

            let text = r#"{"e":"bookTicker"}"#;
            common
                .on_text_frame(text.to_string(), Arc::clone(&connection), 1, None)
                .await;
            assert_eq!(&*order.lock().unwrap(), &["raw", "handler"]);
            assert_eq!(&payloads.lock().unwrap()[0], text.as_bytes());

            raw_seen.store(false, Ordering::SeqCst);
            order.lock().unwrap().clear();
            let binary_text = r#"{"e":"depthUpdate"}"#;
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(binary_text.as_bytes()).unwrap();
            let compressed = encoder.finish().unwrap();
            common
                .on_binary_frame(&compressed, Arc::clone(&connection), 1, None)
                .await;

            assert_eq!(&*order.lock().unwrap(), &["raw", "handler"]);
            assert_eq!(&payloads.lock().unwrap()[1], &compressed);
            assert_eq!(
                &*messages.lock().unwrap(),
                &[text.to_string(), binary_text.to_string()]
            );
        });
    }

    #[test]
    fn context_observer_preserves_scope_generation_kind_and_raw_bytes() {
        TOKIO_SHARED_RT.block_on(async {
            type SeenFrame = (String, u64, Option<String>, RawFrameKind, Vec<u8>);

            let raw_seen = Arc::new(AtomicBool::new(false));
            let order = Arc::new(StdMutex::new(Vec::new()));
            let messages = Arc::new(StdMutex::new(Vec::<String>::new()));
            let frames = Arc::new(StdMutex::new(Vec::<SeenFrame>::new()));
            let observer = {
                let raw_seen = Arc::clone(&raw_seen);
                let order = Arc::clone(&order);
                let frames = Arc::clone(&frames);
                RawFrameObserver::new_context(move |context| {
                    frames.lock().unwrap().push((
                        context.connection_id.to_string(),
                        context.session_generation,
                        context.path_scope.map(str::to_string),
                        context.kind,
                        context.payload.to_vec(),
                    ));
                    order.lock().unwrap().push("raw");
                    raw_seen.store(true, Ordering::SeqCst);
                })
            };
            let connection = WebsocketConnection::new("context-slot");
            connection
                .set_handler(Arc::new(RawOrderHandler {
                    raw_seen: Arc::clone(&raw_seen),
                    order: Arc::clone(&order),
                    messages: Arc::clone(&messages),
                }))
                .await;
            let common = WebsocketCommon::new_with_raw_frame_observer(
                vec![Arc::clone(&connection)],
                WebsocketMode::Single,
                0,
                None,
                None,
                Some(observer),
            );

            let text = r#"{"e":"bookTicker"}"#;
            common
                .on_text_frame(text.to_string(), Arc::clone(&connection), 7, Some("public"))
                .await;
            raw_seen.store(false, Ordering::SeqCst);
            order.lock().unwrap().clear();

            let binary_text = r#"{"e":"depthUpdate"}"#;
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(binary_text.as_bytes()).unwrap();
            let compressed = encoder.finish().unwrap();
            common
                .on_binary_frame(&compressed, Arc::clone(&connection), 7, Some("public"))
                .await;

            assert_eq!(&*order.lock().unwrap(), &["raw", "handler"]);
            assert_eq!(
                &*frames.lock().unwrap(),
                &[
                    (
                        "context-slot".to_string(),
                        7,
                        Some("public".to_string()),
                        RawFrameKind::Text,
                        text.as_bytes().to_vec(),
                    ),
                    (
                        "context-slot".to_string(),
                        7,
                        Some("public".to_string()),
                        RawFrameKind::Binary,
                        compressed,
                    ),
                ]
            );
            assert_eq!(
                &*messages.lock().unwrap(),
                &[text.to_string(), binary_text.to_string()]
            );
        });
    }

    #[test]
    fn arbitrary_scope_is_redacted_at_raw_and_lifecycle_observer_boundaries() {
        TOKIO_SHARED_RT.block_on(async {
            const SECRET_PATH: &str = "listen-key-in-arbitrary-path";
            let raw_observed = Arc::new(StdMutex::new(Vec::new()));
            let lifecycle_observed = Arc::new(StdMutex::new(Vec::new()));
            let observer = {
                let raw_observed = Arc::clone(&raw_observed);
                let lifecycle_observed = Arc::clone(&lifecycle_observed);
                RawFrameObserver::new_context(move |context| {
                    raw_observed.lock().unwrap().push((
                        context.path_scope.map(str::to_string),
                        format!("{context:?}"),
                    ));
                })
                .with_lifecycle(move |context| {
                    lifecycle_observed.lock().unwrap().push((
                        context.path_scope.map(str::to_string),
                        format!("{context:?}"),
                    ));
                })
            };
            let connection = WebsocketConnection::new("redacted-scope-slot");
            let common = WebsocketCommon::new_with_raw_frame_observer(
                vec![Arc::clone(&connection)],
                WebsocketMode::Single,
                0,
                None,
                None,
                Some(observer),
            );

            common
                .on_text_frame("{}".to_string(), connection, 9, Some(SECRET_PATH))
                .await;
            common.observe_lifecycle(
                "redacted-scope-slot",
                9,
                Some(SECRET_PATH),
                WebsocketLifecycleEvent::Open,
            );

            let raw_observed = raw_observed.lock().unwrap();
            assert_eq!(raw_observed[0].0.as_deref(), Some("other"));
            assert!(!raw_observed[0].1.contains(SECRET_PATH));
            let lifecycle_observed = lifecycle_observed.lock().unwrap();
            assert_eq!(lifecycle_observed[0].0.as_deref(), Some("other"));
            assert!(!lifecycle_observed[0].1.contains(SECRET_PATH));
        });
    }

    #[test]
    fn lifecycle_context_distinguishes_overlapping_physical_sessions() {
        TOKIO_SHARED_RT.block_on(async {
            let observed = Arc::new(StdMutex::new(Vec::new()));
            let observer = {
                let observed = Arc::clone(&observed);
                RawFrameObserver::new_context(|_| {}).with_lifecycle(move |context| {
                    observed.lock().unwrap().push((
                        context.connection_id.to_string(),
                        context.session_generation,
                        context.path_scope.map(str::to_string),
                        context.event,
                    ));
                })
            };
            let common = WebsocketCommon::new_with_raw_frame_observer(
                vec![WebsocketConnection::new("renewed-slot")],
                WebsocketMode::Single,
                0,
                None,
                None,
                Some(observer),
            );

            common.observe_lifecycle(
                "renewed-slot",
                1,
                Some("private"),
                WebsocketLifecycleEvent::Open,
            );
            common.observe_lifecycle(
                "renewed-slot",
                2,
                Some("private"),
                WebsocketLifecycleEvent::Open,
            );
            common.observe_lifecycle(
                "renewed-slot",
                1,
                Some("private"),
                WebsocketLifecycleEvent::Close { code: 1000 },
            );

            assert_eq!(
                &*observed.lock().unwrap(),
                &[
                    (
                        "renewed-slot".to_string(),
                        1,
                        Some("private".to_string()),
                        WebsocketLifecycleEvent::Open,
                    ),
                    (
                        "renewed-slot".to_string(),
                        2,
                        Some("private".to_string()),
                        WebsocketLifecycleEvent::Open,
                    ),
                    (
                        "renewed-slot".to_string(),
                        1,
                        Some("private".to_string()),
                        WebsocketLifecycleEvent::Close { code: 1000 },
                    ),
                ]
            );
        });
    }

    #[test]
    fn websocket_url_redaction_never_retains_stream_credentials() {
        let private = redacted_websocket_url(
            "wss://fstream.binance.com/private/stream?streams=SECRET-LISTEN-KEY",
        );
        assert_eq!(private, "wss://fstream.binance.com/private/<redacted>");
        assert!(!private.contains("SECRET"));

        let legacy = redacted_websocket_url("wss://fstream.binance.com/ws/SECRET-LISTEN-KEY");
        assert_eq!(legacy, "wss://fstream.binance.com/ws/<redacted>");
        assert!(!legacy.contains("SECRET"));

        let unknown = redacted_websocket_url("wss://example.com/SECRET-LISTEN-KEY");
        assert_eq!(unknown, "wss://example.com/<redacted>");
        assert!(!unknown.contains("SECRET"));
        assert_eq!(
            redacted_websocket_url("not-a-url"),
            "<invalid websocket URL>"
        );
    }

    #[test]
    fn configured_raw_observer_captures_frames_that_fail_in_the_public_reader() {
        TOKIO_SHARED_RT.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let malformed_json = b"{not-json".to_vec();
            let invalid_zlib = b"not-a-zlib-stream".to_vec();
            assert!(serde_json::from_slice::<Value>(&malformed_json).is_err());
            let mut decoded = String::new();
            assert!(
                ZlibDecoder::new(invalid_zlib.as_slice())
                    .read_to_string(&mut decoded)
                    .is_err(),
                "fixture must fail zlib decompression"
            );
            let server_json = malformed_json.clone();
            let server_zlib = invalid_zlib.clone();

            let _server = AbortOnDrop(tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();
                ws.send(Message::Text(
                    String::from_utf8(server_json).unwrap().into(),
                ))
                .await
                .unwrap();
                ws.send(Message::Binary(server_zlib.into())).await.unwrap();
                ws.close(None).await.unwrap();
            }));

            let payloads = Arc::new(StdMutex::new(Vec::<Vec<u8>>::new()));
            let observer = {
                let payloads = Arc::clone(&payloads);
                RawFrameObserver::new(move |connection_id, payload| {
                    assert_eq!(connection_id, "raw-public-reader");
                    payloads.lock().unwrap().push(payload.to_vec());
                })
            };
            let config = ConfigurationWebsocketStreams {
                ws_url: Some(format!("ws://{addr}")),
                mode: WebsocketMode::Single,
                reconnect_delay: 500,
                time_unit: None,
                raw_frame_observer: Some(observer),
                stream_subscription_observer: None,
                agent: None,
                user_agent: build_user_agent("raw-reader-test"),
            };
            let streams = WebsocketStreams::new(
                config,
                vec![WebsocketConnection::new("raw-public-reader")],
                vec![],
            );

            Arc::clone(&streams)
                .connect(vec!["btcusdt@bookTicker".to_string()])
                .await
                .unwrap();
            assert!(
                eventually_async(Duration::from_secs(1), || {
                    let payloads = Arc::clone(&payloads);
                    async move { payloads.lock().unwrap().len() == 2 }
                })
                .await,
                "reader did not pass both failing frames to the configured observer"
            );

            assert_eq!(&*payloads.lock().unwrap(), &[malformed_json, invalid_zlib]);
        });
    }

    #[test]
    fn open_precedes_first_frame_and_one_failure_schedules_one_reconnect() {
        TOKIO_SHARED_RT.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let _server = AbortOnDrop(tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut ws = accept_async(stream).await.unwrap();
                ws.send(Message::Text(
                    r#"{"stream":"btcusdt@bookTicker","data":{}}"#.into(),
                ))
                .await
                .unwrap();
                ws.send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: "rotate".into(),
                })))
                .await
                .unwrap();
            }));

            let observations = Arc::new(StdMutex::new(Vec::<(u64, String)>::new()));
            let observer = {
                let frame_observations = Arc::clone(&observations);
                let lifecycle_observations = Arc::clone(&observations);
                RawFrameObserver::new_context(move |context| {
                    assert_eq!(context.path_scope, Some("public"));
                    frame_observations
                        .lock()
                        .unwrap()
                        .push((context.session_generation, "frame".to_string()));
                })
                .with_lifecycle(move |context| {
                    assert_eq!(context.path_scope, Some("public"));
                    let name = match context.event {
                        WebsocketLifecycleEvent::Open => "open",
                        WebsocketLifecycleEvent::Close { .. } => "close",
                        WebsocketLifecycleEvent::ReconnectScheduled { .. } => "reconnect",
                        _ => return,
                    };
                    lifecycle_observations
                        .lock()
                        .unwrap()
                        .push((context.session_generation, name.to_string()));
                })
            };
            let config = ConfigurationWebsocketStreams {
                ws_url: Some(format!("ws://{addr}")),
                mode: WebsocketMode::Single,
                reconnect_delay: 5_000,
                time_unit: None,
                raw_frame_observer: Some(observer),
                stream_subscription_observer: None,
                agent: None,
                user_agent: build_user_agent("scoped-observer-test"),
            };
            let streams = WebsocketStreams::new(
                config,
                vec![WebsocketConnection::new("scoped-slot")],
                vec!["public".to_string()],
            );

            Arc::clone(&streams)
                .connect(vec!["btcusdt@bookTicker".to_string()])
                .await
                .unwrap();
            assert!(
                eventually_async(Duration::from_secs(1), || {
                    let observations = Arc::clone(&observations);
                    async move {
                        observations
                            .lock()
                            .unwrap()
                            .iter()
                            .any(|(_, event)| event == "reconnect")
                    }
                })
                .await,
                "reader did not expose the reconnect transition"
            );
            sleep(Duration::from_millis(50)).await;

            let observed = observations.lock().unwrap().clone();
            let open = observed
                .iter()
                .position(|(_, event)| event == "open")
                .unwrap();
            let frame = observed
                .iter()
                .position(|(_, event)| event == "frame")
                .unwrap();
            assert!(open < frame, "Open must be observed before the first frame");
            assert!(observed.iter().all(|(generation, _)| *generation == 1));
            assert_eq!(
                observed
                    .iter()
                    .filter(|(_, event)| event == "reconnect")
                    .count(),
                1,
                "one reader failure must enqueue one reconnect"
            );
        });
    }

    /// Spawn a mock WebSocket listener that accepts a single connection and
    /// holds it open until the client disconnects, then exits. The returned
    /// guard aborts the task when dropped.
    fn spawn_mock_ws_listener(listener: TcpListener) -> AbortOnDrop {
        AbortOnDrop(tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let Ok(mut ws) = accept_async(stream).await else {
                    return;
                };
                while ws.next().await.is_some() {}
            }
        }))
    }

    fn subscribe_events(common: &WebsocketCommon) -> Arc<Mutex<Vec<WebsocketEvent>>> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_clone = events.clone();
        common.events.subscribe(move |event| {
            let events_clone = events_clone.clone();
            tokio::spawn(async move {
                events_clone.lock().await.push(event);
            });
        });
        events
    }

    async fn create_connection(
        id: &str,
        has_writer: bool,
        reconnection_pending: bool,
        renewal_pending: bool,
        close_initiated: bool,
    ) -> Arc<WebsocketConnection> {
        let conn = WebsocketConnection::new(id);
        let mut st = conn.state.lock().await;
        st.reconnection_pending = reconnection_pending;
        st.renewal_pending = renewal_pending;
        st.close_initiated = close_initiated;
        if has_writer {
            let (tx, _) = unbounded_channel::<Message>();
            st.ws_write_tx = Some(tx);
        } else {
            st.ws_write_tx = None;
        }
        drop(st);
        conn
    }

    fn create_websocket_api(
        time_unit: Option<TimeUnit>,
        mode: Option<WebsocketMode>,
        auto_session_relogon: Option<bool>,
    ) -> Arc<WebsocketApi> {
        let mode = mode.unwrap_or(WebsocketMode::Single);
        let auto_session_relogon = auto_session_relogon.unwrap_or(true);
        let sig_gen = SignatureGenerator::new(
            Some("api_secret".into()),
            None::<PrivateKey>,
            None::<String>,
        );
        let config = ConfigurationWebsocketApi {
            api_key: Some("api_key".into()),
            api_secret: Some("api_secret".into()),
            private_key: None,
            private_key_passphrase: None,
            ws_url: Some("wss://example.com".into()),
            mode,
            reconnect_delay: 1000,
            signature_gen: sig_gen,
            timeout: 500,
            time_unit,
            auto_session_relogon,
            agent: None,
            user_agent: build_user_agent("product"),
        };
        let conn1 = WebsocketConnection::new("c1");
        let conn2 = WebsocketConnection::new("c2");
        WebsocketApi::new(config, vec![conn1, conn2])
    }

    fn create_websocket_streams(
        ws_url: Option<&str>,
        conns: Option<Vec<Arc<WebsocketConnection>>>,
        url_paths: Option<Vec<String>>,
    ) -> Arc<WebsocketStreams> {
        let mut connections: Vec<Arc<WebsocketConnection>> = vec![];
        let url_paths = url_paths.unwrap_or_default();
        if conns.is_none() {
            connections.push(WebsocketConnection::new("c1"));
            connections.push(WebsocketConnection::new("c2"));
        } else {
            connections = conns.expect("Expected connections to be set");
        }
        let config = ConfigurationWebsocketStreams {
            ws_url: Some(ws_url.unwrap_or("example.com").to_string()),
            mode: WebsocketMode::Single,
            reconnect_delay: 500,
            time_unit: None,
            raw_frame_observer: None,
            stream_subscription_observer: None,
            agent: None,
            user_agent: build_user_agent("product"),
        };
        WebsocketStreams::new(config, connections, url_paths)
    }

    fn subscribe_to_emitter(emitter: &WebsocketEventEmitter) -> Receiver<WebsocketEvent> {
        let (test_tx, test_rx) = tokio::sync::mpsc::channel(16);
        let _sub = emitter.subscribe(move |evt| {
            let _ = test_tx.try_send(evt);
        });
        test_rx
    }

    async fn expect_websocket_event(rx: &mut Receiver<WebsocketEvent>) -> WebsocketEvent {
        timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("subscriber channel closed")
    }

    async fn eventually_async<F, Fut>(max_wait: Duration, mut f: F) -> bool
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let start = tokio::time::Instant::now();
        while start.elapsed() < max_wait {
            if f().await {
                return true;
            }
            sleep(Duration::from_millis(20)).await;
        }
        false
    }

    mod event_emitter {
        use super::*;

        #[test]
        fn event_emitter_subscribe_and_emit() {
            TOKIO_SHARED_RT.block_on(async {
                let emitter = WebsocketEventEmitter::new();
                let (tx, rx) = oneshot::channel();
                let tx = Arc::new(std::sync::Mutex::new(Some(tx)));
                let tx_clone = tx.clone();
                let _sub = emitter.subscribe(move |event| {
                    if let Some(sender) = tx_clone.lock().unwrap().take() {
                        let _ = sender.send(event);
                    }
                });
                emitter.emit(&WebsocketEvent::Open);
                let received = timeout(Duration::from_millis(100), rx)
                    .await
                    .expect("timed out");
                assert_eq!(received, Ok(WebsocketEvent::Open));
            });
        }

        #[test]
        fn single_subscriber_gets_event() {
            TOKIO_SHARED_RT.block_on(async {
                let emitter = WebsocketEventEmitter::new();
                let mut rx = subscribe_to_emitter(&emitter);

                let e1 = WebsocketEvent::Open;
                emitter.emit(&e1);

                let got = expect_websocket_event(&mut rx).await;
                assert_eq!(got, e1);
            });
        }

        #[test]
        fn multiple_subscribers_get_event() {
            TOKIO_SHARED_RT.block_on(async {
                let emitter = WebsocketEventEmitter::new();
                let mut rx1 = subscribe_to_emitter(&emitter);
                let mut rx2 = subscribe_to_emitter(&emitter);

                let e = WebsocketEvent::Message("hello".into());
                emitter.emit(&e);

                assert_eq!(expect_websocket_event(&mut rx1).await, e.clone());
                assert_eq!(expect_websocket_event(&mut rx2).await, e);
            });
        }

        #[test]
        fn closed_subscribers_are_pruned() {
            TOKIO_SHARED_RT.block_on(async {
                let emitter = WebsocketEventEmitter::new();
                let rx1 = subscribe_to_emitter(&emitter);
                let mut rx2 = subscribe_to_emitter(&emitter);
                drop(rx1);

                let e = WebsocketEvent::Pong;
                emitter.emit(&e);

                assert_eq!(expect_websocket_event(&mut rx2).await, e);
            });
        }

        #[test]
        fn prune_on_error_does_not_hang() {
            TOKIO_SHARED_RT.block_on(async {
                let emitter = WebsocketEventEmitter::new();
                let rx = subscribe_to_emitter(&emitter);
                drop(rx);

                let e = WebsocketEvent::Close(1000, "bye".into());
                emitter.emit(&e);
            });
        }
    }

    mod websocket_common {
        use super::*;

        mod initialisation {
            use super::*;

            #[test]
            fn single_mode() {
                TOKIO_SHARED_RT.block_on(async {
                    let common = WebsocketCommon::new(vec![], WebsocketMode::Single, 0, None, None);
                    assert_eq!(common.connection_pool.len(), 1);
                });
            }

            #[test]
            fn pool_mode() {
                TOKIO_SHARED_RT.block_on(async {
                    let common =
                        WebsocketCommon::new(vec![], WebsocketMode::Pool(3), 0, None, None);
                    assert_eq!(common.connection_pool.len(), 3);
                });
            }
        }

        mod spawn_reconnect_loop {
            use super::*;

            struct ReplacementReconnectUrl(String);

            #[async_trait]
            impl WebsocketHandler for ReplacementReconnectUrl {
                async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {}

                async fn on_message(&self, _data: String, _connection: Arc<WebsocketConnection>) {}

                async fn get_reconnect_url(
                    &self,
                    _default_url: String,
                    _connection: Arc<WebsocketConnection>,
                ) -> String {
                    self.0.clone()
                }
            }

            #[test]
            fn successful_reconnect_entry_triggers_init_connect() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            sleep(Duration::from_secs(5)).await;
                            let _ = ws.close(None).await;
                        }
                    });

                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        10,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: "c1".into(),
                            session_generation: 0,
                            path_scope: None,
                            url: url.clone(),
                            is_renewal: false,
                        })
                        .await
                        .unwrap();

                    let mut ok = false;
                    for _ in 0..100 {
                        if conn.state.lock().await.ws_write_tx.is_some() {
                            ok = true;
                            break;
                        }
                        sleep(Duration::from_millis(50)).await;
                    }
                    assert!(ok, "expected ws_write_tx to be Some after reconnect");
                });
            }

            #[test]
            fn reconnect_recomputes_url_immediately_before_attempt() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);

                    let conn = WebsocketConnection::new("recompute-url");
                    conn.set_handler(Arc::new(ReplacementReconnectUrl(format!(
                        "ws://{addr}/current"
                    ))))
                    .await;
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: conn.id.clone(),
                            session_generation: 0,
                            path_scope: Some("private".to_string()),
                            url: "ws://127.0.0.1:1/stale-listen-key".to_string(),
                            is_renewal: false,
                        })
                        .await
                        .unwrap();

                    assert!(
                        eventually_async(Duration::from_secs(1), || {
                            let conn = Arc::clone(&conn);
                            async move { conn.state.lock().await.ws_write_tx.is_some() }
                        })
                        .await,
                        "reconnect attempt did not use the handler's current URL"
                    );
                });
            }

            #[test]
            fn reconnect_entry_with_unknown_id_is_ignored() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            let _ = ws.close(None).await;
                        }
                    });

                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        5,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: "other".into(),
                            session_generation: 0,
                            path_scope: None,
                            url,
                            is_renewal: false,
                        })
                        .await
                        .unwrap();

                    sleep(Duration::from_secs(1)).await;

                    let st = conn.state.lock().await;
                    assert!(st.ws_write_tx.is_none());
                });
            }

            #[test]
            fn stale_reconnect_entry_is_discarded_before_lifecycle_or_socket_open() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);

                    let observed = Arc::new(StdMutex::new(Vec::new()));
                    let observer = {
                        let observed = Arc::clone(&observed);
                        RawFrameObserver::new_context(|_| {}).with_lifecycle(move |context| {
                            if matches!(
                                context.event,
                                WebsocketLifecycleEvent::ReconnectScheduled { .. }
                            ) {
                                observed.lock().unwrap().push(context.session_generation);
                            }
                        })
                    };
                    let conn = WebsocketConnection::new("stale-reconnect");
                    conn.session_generation.store(2, Ordering::Release);
                    let common = WebsocketCommon::new_with_raw_frame_observer(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                        Some(observer),
                    );
                    let url = format!("ws://{addr}");
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: conn.id.clone(),
                            session_generation: 1,
                            path_scope: Some("private".to_string()),
                            url: "ws://127.0.0.1:1/SECRET-STALE".to_string(),
                            is_renewal: false,
                        })
                        .await
                        .unwrap();
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: conn.id.clone(),
                            session_generation: 2,
                            path_scope: Some("public".to_string()),
                            url,
                            is_renewal: false,
                        })
                        .await
                        .unwrap();

                    assert!(
                        eventually_async(Duration::from_secs(1), || {
                            let observed = Arc::clone(&observed);
                            async move { observed.lock().unwrap().as_slice() == [2] }
                        })
                        .await,
                        "current reconnect was not observed after stale entry"
                    );
                    assert_eq!(&*observed.lock().unwrap(), &[2]);
                });
            }

            #[test]
            fn renewal_entries_bypass_initial_delay() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            let _ = ws.close(None).await;
                        }
                    });

                    let conn = WebsocketConnection::new("renew");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        200,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: "renew".into(),
                            session_generation: 0,
                            path_scope: None,
                            url: url.clone(),
                            is_renewal: true,
                        })
                        .await
                        .unwrap();

                    sleep(Duration::from_secs(2)).await;

                    let st = conn.state.lock().await;

                    assert!(st.ws_write_tx.is_some());
                });
            }

            #[test]
            fn non_renewal_entries_respect_initial_delay() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);

                    let reconnect_delay_ms = 1500;
                    let conn = WebsocketConnection::new("nonrenew");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        reconnect_delay_ms,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    let queued_at = tokio::time::Instant::now();
                    common
                        .reconnect_tx
                        .send(ReconnectEntry {
                            connection_id: "nonrenew".into(),
                            session_generation: 0,
                            path_scope: None,
                            url: url.clone(),
                            is_renewal: false,
                        })
                        .await
                        .unwrap();

                    sleep(Duration::from_millis(100)).await;
                    if queued_at.elapsed()
                        < Duration::from_millis(reconnect_delay_ms as u64)
                            .saturating_sub(Duration::from_millis(200))
                    {
                        assert!(conn.state.lock().await.ws_write_tx.is_none());
                    }

                    let mut ok = false;
                    for _ in 0..200 {
                        if conn.state.lock().await.ws_write_tx.is_some() {
                            ok = true;
                            break;
                        }
                        sleep(Duration::from_millis(50)).await;
                    }
                    assert!(
                        ok,
                        "expected ws_write_tx to be Some after reconnect delay elapsed"
                    );
                });
            }
        }

        mod spawn_renewal_loop {
            use super::*;

            #[tokio::test]
            async fn scheduling_renewal_does_not_panic_for_known_connection() {
                pause();

                let conn = WebsocketConnection::new("known");
                let common =
                    WebsocketCommon::new(vec![conn.clone()], WebsocketMode::Single, 0, None, None);
                let url = "wss://example".to_string();
                common
                    .renewal_tx
                    .send(RenewalEntry {
                        connection_id: conn.id.clone(),
                        session_generation: 0,
                        path_scope: None,
                        url,
                    })
                    .await
                    .unwrap();
                advance(Duration::from_secs(23 * 60 * 60 + 1)).await;
                resume();
            }

            #[tokio::test]
            async fn scheduling_renewal_ignored_for_unknown_connection() {
                pause();

                let conn = WebsocketConnection::new("c1");
                let common =
                    WebsocketCommon::new(vec![conn.clone()], WebsocketMode::Single, 0, None, None);
                common
                    .renewal_tx
                    .send(RenewalEntry {
                        connection_id: "other".into(),
                        session_generation: 0,
                        path_scope: None,
                        url: "u".into(),
                    })
                    .await
                    .unwrap();
                advance(Duration::from_secs(23 * 60 * 60 + 1)).await;

                resume();
            }

            #[tokio::test]
            async fn stale_renewal_timer_is_discarded() {
                pause();

                let observed = Arc::new(StdMutex::new(Vec::new()));
                let observer = {
                    let observed = Arc::clone(&observed);
                    RawFrameObserver::new_context(|_| {}).with_lifecycle(move |context| {
                        if matches!(
                            context.event,
                            WebsocketLifecycleEvent::ReconnectScheduled { .. }
                        ) {
                            observed.lock().unwrap().push(context.session_generation);
                        }
                    })
                };
                let conn = WebsocketConnection::new("stale-renewal");
                conn.session_generation.store(2, Ordering::Release);
                let common = WebsocketCommon::new_with_raw_frame_observer(
                    vec![conn.clone()],
                    WebsocketMode::Single,
                    0,
                    None,
                    None,
                    Some(observer),
                );
                common
                    .renewal_tx
                    .send(RenewalEntry {
                        connection_id: conn.id.clone(),
                        session_generation: 1,
                        path_scope: Some("private".to_string()),
                        url: "ws://127.0.0.1:1/SECRET-STALE".to_string(),
                    })
                    .await
                    .unwrap();

                advance(MAX_CONN_DURATION + Duration::from_secs(1)).await;
                for _ in 0..10 {
                    tokio::task::yield_now().await;
                }
                assert!(observed.lock().unwrap().is_empty());

                resume();
            }
        }

        mod reconnect_regressions {
            use super::*;

            #[test]
            fn init_connect_is_not_skipped_when_reconnection_pending() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            tokio::spawn(async move {
                                let mut ws = accept_async(stream).await.unwrap();
                                sleep(Duration::from_millis(500)).await;
                                let _ = ws.close(None).await;
                            });
                        }
                    });

                    let conn = WebsocketConnection::new("c-reconnect");
                    {
                        let mut st = conn.state.lock().await;
                        let (tx, _) = unbounded_channel::<Message>();
                        st.ws_write_tx = Some(tx);
                        st.reconnection_pending = true;
                    }

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );

                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await
                        .unwrap();

                    let ok = eventually_async(Duration::from_secs(2), || {
                        let conn = conn.clone();
                        async move {
                            let st = conn.state.lock().await;
                            st.ws_write_tx.is_some() && !st.reconnection_pending
                        }
                    })
                    .await;

                    assert!(
                        ok,
                        "expected writer installed and reconnection_pending cleared"
                    );
                });
            }

            #[test]
            fn pending_request_is_resolved_on_socket_drop() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            let _ = ws.next().await;
                            let _ = ws.close(None).await;
                        }
                    });

                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );

                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await
                        .unwrap();

                    let rx = common
                        .send(
                            "{\"id\":\"req-1\",\"method\":\"PING\"}".to_string(),
                            Some("req-1".to_string()),
                            true,
                            Duration::from_millis(150),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap()
                        .expect("expected oneshot receiver");

                    let res = timeout(Duration::from_secs(2), rx)
                        .await
                        .expect("did not resolve pending request")
                        .expect("oneshot cancelled");

                    assert!(matches!(res, Err(WebsocketError::Timeout)));

                    let ok = eventually_async(Duration::from_secs(1), || {
                        let conn = conn.clone();
                        async move { conn.state.lock().await.pending_requests.is_empty() }
                    })
                    .await;

                    assert!(ok, "pending_requests should be drained");
                });
            }
        }

        mod is_connection_ready {
            use super::*;

            #[test]
            fn is_connection_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    assert!(!common.is_connection_ready(&conn, false).await);
                    assert!(common.is_connection_ready(&conn, true).await);
                });
            }

            #[test]
            fn connection_ready_basic() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = create_connection("c1", true, false, false, false).await;
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    assert!(common.is_connection_ready(&conn, false).await);
                });
            }

            #[test]
            fn connection_not_ready_without_writer() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = create_connection("c1", false, false, false, false).await;
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    assert!(!common.is_connection_ready(&conn, false).await);
                    assert!(common.is_connection_ready(&conn, true).await);
                });
            }

            #[test]
            fn connection_not_ready_when_flagged() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = create_connection("c1", true, true, false, false).await;
                    let conn2 = create_connection("c2", true, false, true, false).await;
                    let conn3 = create_connection("c3", true, false, false, true).await;

                    let common = WebsocketCommon::new(
                        vec![conn1.clone(), conn2.clone(), conn3.clone()],
                        WebsocketMode::Pool(3),
                        0,
                        None,
                        None,
                    );

                    assert!(!common.is_connection_ready(&conn1, false).await);
                    assert!(common.is_connection_ready(&conn2, false).await);
                    assert!(!common.is_connection_ready(&conn3, false).await);
                });
            }
        }

        mod is_connected {
            use super::*;

            #[test]
            fn with_pool_various_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn_a = create_connection("a", true, false, false, false).await;
                    let conn_b = create_connection("b", false, false, false, false).await;
                    let conn_c = create_connection("c", true, true, false, false).await;
                    let pool = vec![conn_a.clone(), conn_b.clone(), conn_c.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Pool(3), 0, None, None);

                    assert!(common.is_connected(None).await);
                    assert!(common.is_connected(Some(&conn_a)).await);
                    assert!(!common.is_connected(Some(&conn_b)).await);
                    assert!(!common.is_connected(Some(&conn_c)).await);
                });
            }

            #[test]
            fn with_pool_all_bad_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let bad1 = create_connection("c1", false, false, false, false).await;
                    let bad2 = create_connection("c2", true, true, false, false).await;
                    let bad3 = create_connection("c3", true, false, false, true).await;
                    let common = WebsocketCommon::new(
                        vec![bad1, bad2, bad3],
                        WebsocketMode::Pool(3),
                        0,
                        None,
                        None,
                    );

                    assert!(!common.is_connected(None).await);
                });
            }

            #[test]
            fn with_pool_ignore_close_initiated() {
                TOKIO_SHARED_RT.block_on(async {
                    let good = create_connection("c1", true, false, false, false).await;
                    let closed = create_connection("c2", true, false, false, true).await;
                    let bad = create_connection("c3", false, false, false, false).await;
                    let common = WebsocketCommon::new(
                        vec![closed.clone(), good.clone(), bad.clone()],
                        WebsocketMode::Pool(3),
                        0,
                        None,
                        None,
                    );

                    assert!(common.is_connected(None).await);
                    assert!(!common.is_connected(Some(&closed)).await);
                });
            }
        }

        mod get_available_connections {
            use super::*;

            #[test]
            fn single_mode() {
                TOKIO_SHARED_RT.block_on(async {
                    let common = WebsocketCommon::new(vec![], WebsocketMode::Single, 0, None, None);
                    let connections = common.get_available_connections(false, None).await;
                    assert_eq!(connections[0].id, common.connection_pool[0].id);
                });
            }

            #[test]
            fn single_mode_with_url_path_does_not_force_first_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");

                    let (tx2, _rx2) = unbounded_channel();
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                        s2.url_path = Some("path1".to_string());
                    }

                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.url_path = Some("path1".to_string());
                    }

                    let pool = vec![conn1.clone(), conn2.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Single, 0, None, None);

                    let connections = common.get_available_connections(false, Some("path1")).await;

                    assert_eq!(connections.len(), 1);
                    assert_eq!(connections[0].id, "c2");
                });
            }

            #[test]
            fn pool_mode_not_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let common =
                        WebsocketCommon::new(vec![], WebsocketMode::Pool(2), 0, None, None);
                    let connections = common.get_available_connections(false, None).await;
                    assert!(connections.is_empty());
                });
            }

            #[test]
            fn pool_mode_with_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx1, _rx1) = unbounded_channel();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                    }
                    let pool = vec![conn1.clone(), conn2.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Pool(2), 0, None, None);
                    let connections = common.get_available_connections(false, None).await;
                    assert!(connections.len() == 1);
                });
            }
        }

        mod get_connection {
            use super::*;

            #[test]
            fn single_mode() {
                TOKIO_SHARED_RT.block_on(async {
                    let common = WebsocketCommon::new(vec![], WebsocketMode::Single, 0, None, None);
                    let conn = common
                        .get_connection(false, None)
                        .await
                        .expect("should get connection");
                    assert_eq!(conn.id, common.connection_pool[0].id);
                });
            }

            #[test]
            fn single_mode_with_url_path_selects_matching_ready_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");

                    let (tx1, _rx1) = unbounded_channel();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                        s1.url_path = Some("path2".to_string());
                    }

                    let (tx2, _rx2) = unbounded_channel();
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                        s2.url_path = Some("path1".to_string());
                    }

                    let pool = vec![conn1.clone(), conn2.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Single, 0, None, None);

                    let chosen = common
                        .get_connection(false, Some("path1"))
                        .await
                        .expect("should get connection");

                    assert_eq!(chosen.id, "c2");
                });
            }

            #[test]
            fn pool_mode_not_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let common =
                        WebsocketCommon::new(vec![], WebsocketMode::Pool(2), 0, None, None);
                    let result = common.get_connection(false, None).await;
                    assert!(matches!(
                        result,
                        Err(crate::errors::WebsocketError::NotConnected)
                    ));
                });
            }

            #[test]
            fn pool_mode_with_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx1, _rx1) = unbounded_channel();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                    }
                    let pool = vec![conn1.clone(), conn2.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Pool(2), 0, None, None);
                    let result = common.get_connection(false, None).await;
                    assert!(result.is_ok());
                    let chosen = result.unwrap();
                    assert_eq!(chosen.id, conn1.id);
                });
            }

            #[test]
            fn pool_mode_with_url_path_filters_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");

                    let (tx1, _rx1) = unbounded_channel();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                        s1.url_path = Some("path1".to_string());
                    }

                    let (tx2, _rx2) = unbounded_channel();
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                        s2.url_path = Some("path2".to_string());
                    }

                    let pool = vec![conn1.clone(), conn2.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Pool(2), 0, None, None);

                    let chosen = common
                        .get_connection(false, Some("path2"))
                        .await
                        .expect("should pick ready connection for path2");

                    assert_eq!(chosen.id, "c2");
                });
            }

            #[test]
            fn url_path_no_match_returns_not_connected() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let (tx1, _rx1) = unbounded_channel();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                        s1.url_path = Some("path1".to_string());
                    }

                    let pool = vec![conn1.clone()];
                    let common = WebsocketCommon::new(pool, WebsocketMode::Pool(1), 0, None, None);

                    let result = common.get_connection(false, Some("path2")).await;
                    assert!(matches!(
                        result,
                        Err(crate::errors::WebsocketError::NotConnected)
                    ));
                });
            }
        }

        mod close_connection_gracefully {
            use super::*;

            #[tokio::test]
            async fn waits_for_pending_requests_then_closes() {
                pause();

                let conn = WebsocketConnection::new("c1");
                let (tx, mut rx) = unbounded_channel::<Message>();
                let (req_tx, _req_rx) = oneshot::channel();
                {
                    let mut st = conn.state.lock().await;
                    st.pending_requests
                        .insert("r".to_string(), PendingRequest { completion: req_tx });
                }
                let common =
                    WebsocketCommon::new(vec![conn.clone()], WebsocketMode::Single, 0, None, None);
                let close_fut = common.close_connection_gracefully(tx.clone(), conn.clone());
                advance(Duration::from_secs(1)).await;
                {
                    let mut st = conn.state.lock().await;
                    st.pending_requests.clear();
                }
                conn.drain_notify.notify_waiters();
                advance(Duration::from_secs(1)).await;
                close_fut.await.unwrap();
                match rx.try_recv() {
                    Ok(Message::Close(_)) => {}
                    other => panic!("expected Close, got {other:?}"),
                }

                resume();
            }

            #[tokio::test]
            async fn force_closes_after_timeout() {
                pause();

                let conn = WebsocketConnection::new("c2");
                let (tx, mut rx) = unbounded_channel::<Message>();
                let (req_tx, _req_rx) = oneshot::channel();
                {
                    let mut st = conn.state.lock().await;
                    st.pending_requests.insert(
                        "request_id".to_string(),
                        PendingRequest { completion: req_tx },
                    );
                }
                let common =
                    WebsocketCommon::new(vec![conn.clone()], WebsocketMode::Single, 0, None, None);
                let close_fut = common.close_connection_gracefully(tx.clone(), conn.clone());
                advance(Duration::from_secs(30)).await;
                close_fut.await.unwrap();
                match rx.try_recv() {
                    Ok(Message::Close(_)) => {}
                    other => panic!("expected Close on timeout, got {other:?}"),
                }

                resume();
            }
        }

        mod get_reconnect_url {
            use super::*;

            struct DummyHandler {
                url: String,
            }

            #[async_trait::async_trait]
            impl WebsocketHandler for DummyHandler {
                async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {}
                async fn on_message(&self, _data: String, _connection: Arc<WebsocketConnection>) {}
                async fn get_reconnect_url(
                    &self,
                    _default_url: String,
                    _connection: Arc<WebsocketConnection>,
                ) -> String {
                    self.url.clone()
                }
            }

            #[test]
            fn returns_default_when_no_handler() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let default = "wss://default".to_string();
                    let result = common.get_reconnect_url(&default, conn.clone()).await;
                    assert_eq!(result, default);
                });
            }

            #[test]
            fn returns_handler_url_when_set() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c2");
                    let handler = Arc::new(DummyHandler {
                        url: "wss://custom".into(),
                    });
                    conn.set_handler(handler).await;
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let default = "wss://default".to_string();
                    let result = common.get_reconnect_url(&default, conn.clone()).await;
                    assert_eq!(result, "wss://custom");
                });
            }
        }

        mod on_open {
            use super::*;

            struct DummyHandler {
                called: Arc<Mutex<bool>>,
                opened_url: Arc<Mutex<Option<String>>>,
            }

            #[async_trait]
            impl WebsocketHandler for DummyHandler {
                async fn on_open(&self, url: String, _connection: Arc<WebsocketConnection>) {
                    let mut flag = self.called.lock().await;
                    *flag = true;
                    let mut store = self.opened_url.lock().await;
                    *store = Some(url);
                }
                async fn on_message(&self, _data: String, _connection: Arc<WebsocketConnection>) {}
                async fn get_reconnect_url(
                    &self,
                    default_url: String,
                    _connection: Arc<WebsocketConnection>,
                ) -> String {
                    default_url
                }
            }

            #[test]
            fn emits_open_and_calls_handler() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let called = Arc::new(Mutex::new(false));
                    let opened_url = Arc::new(Mutex::new(None));
                    let handler = Arc::new(DummyHandler {
                        called: called.clone(),
                        opened_url: opened_url.clone(),
                    });

                    conn.set_handler(handler.clone()).await;
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let events = subscribe_events(&common);
                    common
                        .on_open("wss://example.com".into(), conn.clone(), None)
                        .await;

                    sleep(std::time::Duration::from_millis(10)).await;

                    let evs = events.lock().await;
                    assert!(evs.iter().any(|e| matches!(e, WebsocketEvent::Open)));
                    assert!(*called.lock().await);
                    assert_eq!(
                        opened_url.lock().await.as_deref(),
                        Some("wss://example.com")
                    );
                });
            }

            #[test]
            fn handles_renewal_pending_and_closes_old_writer() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c2");
                    let (old_tx, mut old_rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.renewal_pending = true;
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    common
                        .on_open("url".into(), conn.clone(), Some(old_tx.clone()))
                        .await;
                    assert!(!conn.state.lock().await.renewal_pending);
                    match old_rx.try_recv() {
                        Ok(Message::Close(_)) => {}
                        other => panic!("expected Close, got {other:?}"),
                    }
                });
            }
        }

        mod on_message {
            use super::*;

            struct DummyHandler {
                called_with: Arc<Mutex<Vec<String>>>,
            }

            #[async_trait]
            impl WebsocketHandler for DummyHandler {
                async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {}
                async fn on_message(&self, data: String, _connection: Arc<WebsocketConnection>) {
                    self.called_with.lock().await.push(data);
                }
                async fn get_reconnect_url(
                    &self,
                    default_url: String,
                    _connection: Arc<WebsocketConnection>,
                ) -> String {
                    default_url
                }
            }

            #[test]
            fn emits_message_event_without_handler() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let events = subscribe_events(&common);
                    common.on_message("msg".into(), conn.clone()).await;

                    sleep(Duration::from_millis(10)).await;

                    let locked = events.lock().await;
                    assert!(
                        locked
                            .iter()
                            .any(|e| matches!(e, WebsocketEvent::Message(m) if m == "msg"))
                    );
                });
            }

            #[test]
            fn calls_handler_and_emits_message() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c2");
                    let called = Arc::new(Mutex::new(Vec::new()));
                    let handler = Arc::new(DummyHandler {
                        called_with: called.clone(),
                    });
                    conn.set_handler(handler.clone()).await;

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let events = subscribe_events(&common);
                    common.on_message("msg".into(), conn.clone()).await;

                    sleep(Duration::from_millis(10)).await;

                    let evs = events.lock().await;
                    assert!(
                        evs.iter()
                            .any(|e| matches!(e, WebsocketEvent::Message(m) if m == "msg"))
                    );
                    let msgs = called.lock().await;
                    assert_eq!(msgs.as_slice(), &["msg".to_string()]);
                });
            }

            #[test]
            fn preserves_message_order() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c3");
                    let called = Arc::new(Mutex::new(Vec::new()));
                    let handler = Arc::new(DummyHandler {
                        called_with: called.clone(),
                    });
                    conn.set_handler(handler.clone()).await;

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );

                    for i in 0..20 {
                        common.on_message(format!("msg_{i}"), conn.clone()).await;
                    }

                    let msgs = called.lock().await;
                    let expected: Vec<String> = (0..20).map(|i| format!("msg_{i}")).collect();
                    assert_eq!(msgs.as_slice(), expected.as_slice());
                });
            }

            #[test]
            fn preserves_order_with_slow_handler() {
                use std::sync::atomic::{AtomicU32, Ordering};

                struct SlowHandler {
                    received: Arc<Mutex<Vec<String>>>,
                    concurrent_count: Arc<AtomicU32>,
                    max_concurrent: Arc<AtomicU32>,
                }

                #[async_trait]
                impl WebsocketHandler for SlowHandler {
                    async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {}
                    async fn on_message(
                        &self,
                        data: String,
                        _connection: Arc<WebsocketConnection>,
                    ) {
                        let prev = self.concurrent_count.fetch_add(1, Ordering::SeqCst);
                        self.max_concurrent.fetch_max(prev + 1, Ordering::SeqCst);
                        sleep(Duration::from_millis(1)).await;
                        self.received.lock().await.push(data);
                        self.concurrent_count.fetch_sub(1, Ordering::SeqCst);
                    }
                    async fn get_reconnect_url(
                        &self,
                        default_url: String,
                        _connection: Arc<WebsocketConnection>,
                    ) -> String {
                        default_url
                    }
                }

                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c4");
                    let received = Arc::new(Mutex::new(Vec::new()));
                    let concurrent_count = Arc::new(AtomicU32::new(0));
                    let max_concurrent = Arc::new(AtomicU32::new(0));
                    let handler = Arc::new(SlowHandler {
                        received: received.clone(),
                        concurrent_count: concurrent_count.clone(),
                        max_concurrent: max_concurrent.clone(),
                    });
                    conn.set_handler(handler.clone()).await;

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );

                    for i in 0..10 {
                        common.on_message(format!("msg_{i}"), conn.clone()).await;
                    }

                    let msgs = received.lock().await;
                    let expected: Vec<String> = (0..10).map(|i| format!("msg_{i}")).collect();
                    assert_eq!(msgs.as_slice(), expected.as_slice());
                    assert_eq!(
                        max_concurrent.load(Ordering::SeqCst),
                        1,
                        "messages must be processed sequentially, not concurrently"
                    );
                });
            }
        }

        mod create_websocket {
            use super::*;

            #[test]
            fn successful_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr: SocketAddr = listener.local_addr().unwrap();

                    let expected_ua = build_user_agent("product");
                    let expected_ua_clone = expected_ua.clone();

                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let callback = |req: &Request, resp| {
                                let got = req
                                    .headers()
                                    .get(USER_AGENT)
                                    .expect("no USER_AGENT header in WS handshake")
                                    .to_str()
                                    .expect("invalid USER_AGENT header");
                                assert_eq!(got, expected_ua_clone, "User-Agent mismatch");
                                Ok(resp)
                            };
                            let _ = accept_hdr_async(stream, callback).await.unwrap();
                        }
                    });

                    let url = format!("ws://{addr}");
                    let res =
                        WebsocketCommon::create_websocket(&url, None, Some(expected_ua)).await;
                    assert!(res.is_ok(), "handshake failed: {res:?}");
                });
            }

            #[test]
            fn invalid_url_returns_handshake_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let res =
                        WebsocketCommon::create_websocket("not-a-valid-url", None, None).await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }

            #[test]
            fn unreachable_host_returns_handshake_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let res =
                        WebsocketCommon::create_websocket("ws://127.0.0.1:1", None, None).await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }
        }

        mod connect_pool {
            use super::*;

            #[test]
            fn connects_all_in_pool() {
                TOKIO_SHARED_RT.block_on(async {
                    let pool_size = 3;
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        for _ in 0..pool_size {
                            if let Ok((stream, _)) = listener.accept().await {
                                tokio::spawn(async move {
                                    let mut ws = accept_async(stream).await.unwrap();
                                    sleep(Duration::from_millis(500)).await;
                                    let _ = ws.close(None).await;
                                });
                            }
                        }
                    });
                    let conns: Vec<Arc<WebsocketConnection>> = (0..pool_size)
                        .map(|i| WebsocketConnection::new(format!("c{i}")))
                        .collect();
                    let common = WebsocketCommon::new(
                        conns.clone(),
                        WebsocketMode::Pool(pool_size),
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common.clone().connect_pool(&url, None).await.unwrap();
                    for conn in conns {
                        let mut ok = false;
                        for _ in 0..100 {
                            if conn.state.lock().await.ws_write_tx.is_some() {
                                ok = true;
                                break;
                            }
                            sleep(Duration::from_millis(50)).await;
                        }
                        assert!(ok, "expected ws_write_tx Some after connect");
                    }
                });
            }

            #[test]
            fn fails_if_any_refused() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let pool_size = 3;
                    tokio::spawn(async move {
                        for _ in 0..2 {
                            if let Ok((stream, _)) = listener.accept().await {
                                let mut ws = accept_async(stream).await.unwrap();
                                let _ = ws.close(None).await;
                            }
                        }
                    });
                    let mut conns = Vec::new();
                    let valid_url = format!("ws://{addr}");
                    for i in 0..2 {
                        conns.push(WebsocketConnection::new(format!("c{i}")));
                    }
                    conns.push(WebsocketConnection::new("bad"));
                    let common = WebsocketCommon::new(
                        conns.clone(),
                        WebsocketMode::Pool(pool_size),
                        0,
                        None,
                        None,
                    );
                    let res = common.clone().connect_pool(&valid_url, None).await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }

            #[test]
            fn fails_on_invalid_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let common = WebsocketCommon::new(conns, WebsocketMode::Pool(1), 0, None, None);
                    let res = common.connect_pool("not-a-url", None).await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }

            #[test]
            fn fails_if_mixed_success_and_invalid_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            let _ = ws.close(None).await;
                        }
                    });
                    let good = WebsocketConnection::new("good");
                    let bad = WebsocketConnection::new("bad");
                    let common = WebsocketCommon::new(
                        vec![good, bad],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    let res = common.connect_pool(&url, None).await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }

            #[test]
            fn init_connect_invoked_for_each() {
                TOKIO_SHARED_RT.block_on(async {
                    let pool_size = 2;
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        for _ in 0..pool_size {
                            if let Ok((stream, _)) = listener.accept().await {
                                tokio::spawn(async move {
                                    let mut ws = accept_async(stream).await.unwrap();
                                    sleep(Duration::from_millis(500)).await;
                                    let _ = ws.close(None).await;
                                });
                            }
                        }
                    });
                    let conns: Vec<Arc<WebsocketConnection>> = (0..pool_size)
                        .map(|i| WebsocketConnection::new(format!("c{i}")))
                        .collect();
                    let common = WebsocketCommon::new(
                        conns.clone(),
                        WebsocketMode::Pool(pool_size),
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common.clone().connect_pool(&url, None).await.unwrap();
                    for conn in conns {
                        let mut ok = false;
                        for _ in 0..100 {
                            if conn.state.lock().await.ws_write_tx.is_some() {
                                ok = true;
                                break;
                            }
                            sleep(Duration::from_millis(25)).await;
                        }
                        assert!(ok, "expected ws_write_tx Some for {}", conn.id);
                    }
                });
            }

            #[test]
            fn single_mode_uses_first_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common.connect_pool(&url, None).await.unwrap();
                    let ok = eventually_async(Duration::from_secs(5), || {
                        let conn = conn.clone();
                        async move { conn.state.lock().await.ws_write_tx.is_some() }
                    })
                    .await;

                    assert!(ok, "single mode did not select first connection");
                });
            }

            #[test]
            fn empty_subset_is_ok_and_connects_none() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    tokio::spawn(async move {
                        let _ = addr;
                    });

                    let c1 = WebsocketConnection::new("c1");
                    let c2 = WebsocketConnection::new("c2");

                    let common = WebsocketCommon::new(
                        vec![c1.clone(), c2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );

                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .connect_pool(&url, Some(vec![]))
                        .await
                        .unwrap();

                    assert!(c1.state.lock().await.ws_write_tx.is_none());
                    assert!(c2.state.lock().await.ws_write_tx.is_none());
                });
            }
        }

        mod init_connect {
            use super::*;
            use tokio::sync::mpsc::channel;

            #[test]
            fn pool_mode_none_connection_uses_first() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        for _ in 0..2 {
                            if let Ok((stream, _)) = listener.accept().await {
                                let mut ws = accept_async(stream).await.unwrap();
                                ws.close(None).await.ok();
                            }
                        }
                    });

                    let c1 = WebsocketConnection::new("c1");
                    let c2 = WebsocketConnection::new("c2");
                    let common = WebsocketCommon::new(
                        vec![c1.clone(), c2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");

                    common
                        .clone()
                        .init_connect(&url, false, None, None)
                        .await
                        .unwrap();

                    let ok = eventually_async(Duration::from_secs(5), || {
                        let conn1 = c1.clone();
                        async move { conn1.state.lock().await.ws_write_tx.is_some() }
                    })
                    .await;

                    assert!(ok, "first connection was never selected");
                    assert!(c2.state.lock().await.ws_write_tx.is_none());
                });
            }

            #[test]
            fn writer_channel_can_send_text() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let received = Arc::new(Mutex::new(None::<String>));
                    let received_clone = received.clone();

                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            if let Some(Ok(Message::Text(txt))) = ws.next().await {
                                *received_clone.lock().await = Some(txt.to_string());
                            }
                            ws.close(None).await.ok();
                        }
                    });

                    let conn = WebsocketConnection::new("cw");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await
                        .unwrap();

                    let tx = conn.state.lock().await.ws_write_tx.clone().unwrap();
                    tx.send(Message::Text("ping".into())).unwrap();

                    sleep(Duration::from_millis(50)).await;

                    let lock = received.lock().await;
                    assert_eq!(lock.as_deref(), Some("ping"));
                });
            }

            #[test]
            fn does_not_skip_when_reconnection_pending_even_if_writer_exists() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);

                    let conn = WebsocketConnection::new("c-reconnect");
                    {
                        let mut st = conn.state.lock().await;
                        let (tx, _) = unbounded_channel::<Message>();
                        st.ws_write_tx = Some(tx);
                        st.reconnection_pending = true;
                    }

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );

                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await
                        .unwrap();

                    let st = conn.state.lock().await;
                    assert!(
                        st.ws_write_tx.is_some(),
                        "writer should be set after connect"
                    );
                    assert!(
                        !st.reconnection_pending,
                        "reconnection_pending should be cleared after successful connect"
                    );
                });
            }

            #[test]
            fn responds_to_ping_with_pong() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    let saw_pong = Arc::new(Mutex::new(false));
                    let saw_pong2 = saw_pong.clone();

                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let mut ws = accept_async(stream).await.unwrap();
                            ws.send(Message::Ping(vec![1, 2, 3].into())).await.unwrap();
                            if let Some(Ok(Message::Pong(payload))) = ws.next().await {
                                if payload[..] == [1, 2, 3] {
                                    *saw_pong2.lock().await = true;
                                }
                            }
                            let _ = ws.close(None).await;
                        }
                    });

                    let conn = WebsocketConnection::new("c-ping");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn), None)
                        .await
                        .unwrap();

                    sleep(Duration::from_millis(50)).await;

                    assert!(*saw_pong.lock().await, "server should have seen a Pong");
                });
            }

            #[test]
            fn handshake_error_on_invalid_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c-invalid");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .clone()
                        .init_connect("not-a-url", false, Some(conn.clone()), None)
                        .await;
                    assert!(matches!(res, Err(WebsocketError::Handshake(_))));
                });
            }

            #[test]
            fn skip_if_writer_exists_and_not_renewal() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c-writer");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx.clone());
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .clone()
                        .init_connect("ws://127.0.0.1:1", false, Some(conn.clone()), None)
                        .await;

                    assert!(res.is_ok());
                    assert!(rx.try_recv().is_err());
                });
            }

            #[test]
            fn short_circuit_on_already_renewing() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c-renew");
                    {
                        let mut st = conn.state.lock().await;
                        st.renewal_pending = true;
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .clone()
                        .init_connect("ws://127.0.0.1:1", true, Some(conn.clone()), None)
                        .await;

                    assert!(res.is_ok());
                    assert!(conn.state.lock().await.ws_write_tx.is_none());
                });
            }

            #[test]
            fn queued_init_revalidates_expected_generation_under_slot_lock() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let (accepted_tx, accepted_rx) = oneshot::channel();
                    let _server = AbortOnDrop(tokio::spawn(async move {
                        let Ok((stream, _)) = listener.accept().await else {
                            return;
                        };
                        let _ = accepted_tx.send(());
                        let Ok(mut ws) = accept_async(stream).await else {
                            return;
                        };
                        while ws.next().await.is_some() {}
                    }));

                    let conn = WebsocketConnection::new("locked-generation");
                    conn.session_generation.store(1, Ordering::Release);
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let init_guard = conn.init_lock.lock().await;
                    let url = format!("ws://{addr}");
                    let queued = {
                        let common = Arc::clone(&common);
                        let conn = Arc::clone(&conn);
                        tokio::spawn(async move {
                            common.init_connect(&url, true, Some(conn), Some(1)).await
                        })
                    };

                    tokio::task::yield_now().await;
                    conn.session_generation.store(2, Ordering::Release);
                    drop(init_guard);

                    timeout(Duration::from_secs(1), queued)
                        .await
                        .expect("stale init remained blocked")
                        .expect("stale init task panicked")
                        .expect("stale init should be a no-op");
                    assert!(
                        timeout(Duration::from_millis(50), accepted_rx)
                            .await
                            .is_err(),
                        "stale init opened a socket after its generation changed"
                    );
                    assert_eq!(conn.session_generation.load(Ordering::Acquire), 2);
                });
            }

            #[test]
            fn is_renewal_true_sets_and_clears_flag() {
                struct GatedHandler {
                    gate: Mutex<Option<oneshot::Receiver<()>>>,
                }
                #[async_trait]
                impl WebsocketHandler for GatedHandler {
                    async fn on_open(&self, _url: String, _connection: Arc<WebsocketConnection>) {
                        if let Some(rx) = self.gate.lock().await.take() {
                            let _ = rx.await;
                        }
                    }
                    async fn on_message(
                        &self,
                        _data: String,
                        _connection: Arc<WebsocketConnection>,
                    ) {
                    }
                    async fn get_reconnect_url(
                        &self,
                        url: String,
                        _connection: Arc<WebsocketConnection>,
                    ) -> String {
                        url
                    }
                }

                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);

                    let conn = WebsocketConnection::new("c-new-renew");
                    let (gate_tx, gate_rx) = oneshot::channel();
                    conn.set_handler(Arc::new(GatedHandler {
                        gate: Mutex::new(Some(gate_rx)),
                    }))
                    .await;

                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    let init = {
                        let common = Arc::clone(&common);
                        let conn = Arc::clone(&conn);
                        tokio::spawn(async move {
                            common.init_connect(&url, true, Some(conn), None).await
                        })
                    };

                    assert!(
                        eventually_async(Duration::from_secs(1), || {
                            let conn = Arc::clone(&conn);
                            async move {
                                let state = conn.state.lock().await;
                                state.ws_write_tx.is_some() && state.renewal_pending
                            }
                        })
                        .await,
                        "renewal state was not installed before the on-open gate"
                    );

                    let _ = gate_tx.send(());
                    timeout(Duration::from_secs(1), init)
                        .await
                        .expect("renewal init did not complete")
                        .expect("renewal init task panicked")
                        .expect("renewal init failed");

                    let ok = eventually_async(Duration::from_secs(2), || {
                        let conn = conn.clone();
                        async move { !conn.state.lock().await.renewal_pending }
                    })
                    .await;
                    assert!(ok, "renewal_pending should be cleared in on_open");
                    let st = conn.state.lock().await;
                    assert!(
                        !st.renewal_pending,
                        "renewal_pending should be cleared in on_open"
                    );
                });
            }

            #[test]
            fn current_renewed_session_schedules_reconnect_after_failure() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let server_handle = tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let _first_ws = accept_async(stream).await.unwrap();
                            if let Ok((stream, _)) = listener.accept().await {
                                let second_ws = accept_async(stream).await.unwrap();
                                drop(second_ws);
                            }
                            sleep(Duration::from_secs(10)).await;
                        }
                    });

                    let conn = WebsocketConnection::new("c-needs-renew");
                    let lifecycle = Arc::new(StdMutex::new(Vec::new()));
                    let observer = {
                        let lifecycle = Arc::clone(&lifecycle);
                        RawFrameObserver::new_context(|_| {}).with_lifecycle(move |context| {
                            lifecycle
                                .lock()
                                .unwrap()
                                .push((context.session_generation, context.event));
                        })
                    };
                    let (reconnect_tx, mut reconnect_rx) = channel::<ReconnectEntry>(1);
                    let (renewal_tx, _renewal_rx) = channel::<RenewalEntry>(1);
                    let (terminal_shutdown_tx, _terminal_shutdown_rx) = watch::channel(false);
                    let common = Arc::new(WebsocketCommon {
                        events: WebsocketEventEmitter::new(),
                        mode: WebsocketMode::Single,
                        round_robin_index: AtomicUsize::new(0),
                        connection_pool: vec![conn.clone()],
                        reconnect_tx,
                        renewal_tx,
                        reconnect_delay: 0,
                        agent: None,
                        user_agent: None,
                        raw_frame_observer: Some(observer),
                        automatic_session_replacement: AtomicBool::new(true),
                        renewal_enqueue_count: std::sync::atomic::AtomicU64::new(0),
                        terminal_shutdown: AtomicBool::new(false),
                        terminal_shutdown_tx,
                        background_tasks: StdMutex::new(Vec::new()),
                    });
                    let url = format!("ws://{addr}");
                    let res = common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await;

                    assert!(res.is_ok());

                    {
                        let st = conn.state.lock().await;
                        assert!(st.ws_write_tx.is_some(), "writer should be set");
                    }

                    common
                        .clone()
                        .init_connect(&url, true, Some(conn.clone()), None)
                        .await
                        .expect("Renewal init_connect should succeed");

                    let reconnect = timeout(Duration::from_secs(2), reconnect_rx.recv())
                        .await
                        .expect("renewed session failure should schedule reconnect")
                        .expect("reconnect sender should remain open");
                    assert_eq!(reconnect.session_generation, 2);
                    let open_generations = lifecycle
                        .lock()
                        .unwrap()
                        .iter()
                        .filter_map(|(generation, event)| {
                            matches!(event, WebsocketLifecycleEvent::Open).then_some(*generation)
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(open_generations, [1, 2]);
                    assert!(
                        reconnect_rx.try_recv().is_err(),
                        "renewal transition must not enqueue a stale generation reconnect"
                    );
                    server_handle.abort();
                });
            }

            #[test]
            fn default_connection_selected_when_none_passed() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    let _listener_guard = spawn_mock_ws_listener(listener);
                    let conn = WebsocketConnection::new("c-default");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    let res = common.clone().init_connect(&url, false, None, None).await;

                    assert!(res.is_ok());
                    let ok = eventually_async(Duration::from_secs(5), || {
                        let conn = conn.clone();
                        async move { conn.state.lock().await.ws_write_tx.is_some() }
                    })
                    .await;

                    assert!(ok, "default connection was never selected");
                });
            }

            #[test]
            fn schedules_reconnect_on_abnormal_close() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();
                    tokio::spawn(async move {
                        if let Ok((stream, _)) = listener.accept().await {
                            let ws = accept_async(stream).await.unwrap();

                            // Explicitly sending an AbnormalClose (1006) is not allowed by the websocket
                            // protocol and will be converted into Protocol (1002), which the client receives.
                            //
                            // Abruptly dropping the connection simulates a real abnormal close, which is
                            // handled as an error in the websocket protocol
                            drop(ws);
                        }
                    });
                    let conn = WebsocketConnection::new("c-close");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        5_000,
                        None,
                        None,
                    );
                    let url = format!("ws://{addr}");
                    common
                        .clone()
                        .init_connect(&url, false, Some(conn.clone()), None)
                        .await
                        .unwrap();

                    sleep(Duration::from_millis(50)).await;

                    let st = conn.state.lock().await;
                    assert!(
                        st.reconnection_pending,
                        "expected reconnection_pending to be true after abnormal close"
                    );
                    assert!(
                        st.ws_write_tx.is_none(),
                        "ws_write_tx should be cleared when scheduling a reconnect"
                    );
                });
            }
        }

        mod disconnect {
            use super::*;

            #[test]
            fn returns_ok_when_no_connections_are_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let res = common.disconnect().await;

                    assert!(res.is_ok());
                    assert!(!conn.state.lock().await.close_initiated);
                });
            }

            #[test]
            fn closes_all_ready_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx1, mut rx1) = unbounded_channel::<Message>();
                    let (tx2, mut rx2) = unbounded_channel::<Message>();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                    }
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn1.clone(), conn2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let fut = common.disconnect();

                    sleep(Duration::from_millis(50)).await;

                    fut.await.unwrap();

                    assert!(conn1.state.lock().await.close_initiated);
                    assert!(conn2.state.lock().await.close_initiated);

                    {
                        let st = conn1.state.lock().await;
                        assert!(!st.is_session_logged_on, "conn1 should be logged out");
                        assert!(st.session_logon_req.is_none(), "conn1 req cleared");
                    }
                    {
                        let st = conn2.state.lock().await;
                        assert!(!st.is_session_logged_on, "conn2 should be logged out");
                        assert!(st.session_logon_req.is_none(), "conn2 req cleared");
                    }

                    match (rx1.try_recv(), rx2.try_recv()) {
                        (Ok(Message::Close(_)), Ok(Message::Close(_))) => {}
                        other => panic!("expected two Close frames, got {other:?}"),
                    }
                });
            }

            #[test]
            fn does_not_mark_close_initiated_if_no_writer() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c-new");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    common.disconnect().await.unwrap();

                    assert!(!conn.state.lock().await.close_initiated);
                });
            }

            #[test]
            fn mixed_pool_marks_all_and_closes_only_writers() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn_w = WebsocketConnection::new("with");
                    let conn_wo = WebsocketConnection::new("without");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn_w.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn_w.clone(), conn_wo.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let fut = common.disconnect();

                    sleep(Duration::from_millis(50)).await;

                    fut.await.unwrap();

                    assert!(conn_w.state.lock().await.close_initiated);
                    assert!(conn_wo.state.lock().await.close_initiated);
                    assert!(matches!(rx.try_recv(), Ok(Message::Close(_))));
                });
            }

            #[test]
            fn after_disconnect_not_connected() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, mut _rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    common.disconnect().await.unwrap();
                    assert!(!common.is_connected(Some(&conn)).await);
                });
            }
        }

        mod ping_server {
            use super::*;

            #[test]
            fn sends_ping_to_all_ready_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let mut conns = Vec::new();
                    for i in 0..3 {
                        let conn = WebsocketConnection::new(format!("c{i}"));
                        let (tx, rx) = unbounded_channel::<Message>();
                        {
                            let mut st = conn.state.lock().await;
                            st.ws_write_tx = Some(tx);
                        }
                        conns.push((conn, rx));
                    }
                    let common = WebsocketCommon::new(
                        conns.iter().map(|(c, _)| c.clone()).collect(),
                        WebsocketMode::Pool(3),
                        0,
                        None,
                        None,
                    );
                    common.ping_server().await;
                    for (_, mut rx) in conns {
                        match rx.try_recv() {
                            Ok(Message::Ping(payload)) if payload.is_empty() => {}
                            other => panic!("expected empty-payload Ping, got {other:?}"),
                        }
                    }
                });
            }

            #[test]
            fn skips_not_ready_and_partial() {
                TOKIO_SHARED_RT.block_on(async {
                    let ready = WebsocketConnection::new("ready");
                    let not_ready = WebsocketConnection::new("not-ready");
                    let (tx_r, mut rx_r) = unbounded_channel::<Message>();
                    {
                        let mut st = ready.state.lock().await;
                        st.ws_write_tx = Some(tx_r);
                    }
                    {
                        let mut st = not_ready.state.lock().await;
                        st.ws_write_tx = None;
                    }
                    let common = WebsocketCommon::new(
                        vec![ready.clone(), not_ready.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    common.ping_server().await;
                    match rx_r.try_recv() {
                        Ok(Message::Ping(payload)) if payload.is_empty() => {}
                        other => panic!("expected Ping on ready, got {other:?}"),
                    }
                });
            }

            #[test]
            fn no_ping_when_flags_block() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.reconnection_pending = true;
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    common.ping_server().await;
                    assert!(rx.try_recv().is_err());
                });
            }
        }

        mod send {
            use super::*;

            #[test]
            fn round_robin_send_without_specific() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx1, mut rx1) = unbounded_channel::<Message>();
                    let (tx2, mut rx2) = unbounded_channel::<Message>();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = Some(tx1);
                    }
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn1.clone(), conn2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );

                    let res1 = common
                        .send("a".into(), None, false, Duration::from_secs(1), None)
                        .await
                        .unwrap();
                    assert!(res1.is_none());

                    let res2 = common
                        .send("b".into(), None, false, Duration::from_secs(1), None)
                        .await
                        .unwrap();
                    assert!(res2.is_none());

                    assert_eq!(
                        if let Message::Text(t) = rx1.try_recv().unwrap() {
                            t
                        } else {
                            panic!()
                        },
                        "a"
                    );
                    assert_eq!(
                        if let Message::Text(t) = rx2.try_recv().unwrap() {
                            t
                        } else {
                            panic!()
                        },
                        "b"
                    );
                });
            }

            #[test]
            fn round_robin_skips_not_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx2, mut rx2) = unbounded_channel::<Message>();
                    {
                        let mut s1 = conn1.state.lock().await;
                        s1.ws_write_tx = None;
                    }
                    {
                        let mut s2 = conn2.state.lock().await;
                        s2.ws_write_tx = Some(tx2);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn1.clone(), conn2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .send("bar".into(), None, false, Duration::from_secs(1), None)
                        .await
                        .unwrap();
                    assert!(res.is_none());
                    match rx2.try_recv().unwrap() {
                        Message::Text(t) => assert_eq!(t, "bar"),
                        other => panic!("unexpected {other:?}"),
                    }
                });
            }

            #[test]
            fn sync_send_on_specific_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let (tx2, mut rx2) = unbounded_channel::<Message>();
                    {
                        let mut st = conn2.state.lock().await;
                        st.ws_write_tx = Some(tx2);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn1.clone(), conn2.clone()],
                        WebsocketMode::Pool(2),
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .send(
                            "payload".into(),
                            Some("id".into()),
                            false,
                            Duration::from_secs(1),
                            Some(conn2.clone()),
                        )
                        .await
                        .unwrap();
                    assert!(res.is_none());
                    match rx2.try_recv() {
                        Ok(Message::Text(t)) => assert_eq!(t, "payload"),
                        other => panic!("expected Text, got {other:?}"),
                    }
                });
            }

            #[test]
            fn sync_send_with_id_does_not_insert_pending() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let res = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            false,
                            Duration::from_secs(1),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap();
                    assert!(res.is_none());
                    assert!(conn.state.lock().await.pending_requests.is_empty());
                    match rx.try_recv().unwrap() {
                        Message::Text(t) => assert_eq!(t, "msg"),
                        other => panic!("unexpected {other:?}"),
                    }
                });
            }

            #[test]
            fn sync_send_error_if_not_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let err = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            false,
                            Duration::from_secs(1),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap_err();
                    assert!(matches!(err, WebsocketError::NotConnected));
                });
            }

            #[test]
            fn sync_send_error_when_no_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let err = common
                        .send("msg".into(), None, false, Duration::from_secs(1), None)
                        .await
                        .unwrap_err();
                    assert!(matches!(err, WebsocketError::NotConnected));
                });
            }

            #[test]
            fn async_send_and_receive() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let fut = common
                        .send(
                            "hello".into(),
                            Some("id".into()),
                            true,
                            Duration::from_secs(5),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    match rx.try_recv() {
                        Ok(Message::Text(t)) => assert_eq!(t, "hello"),
                        other => panic!("expected Text, got {other:?}"),
                    }
                    {
                        let mut st = conn.state.lock().await;
                        let pr = st.pending_requests.remove("id").unwrap();
                        pr.completion.send(Ok(serde_json::json!("ok"))).unwrap();
                    }
                    let resp = fut.await.unwrap().unwrap();
                    assert_eq!(resp, serde_json::json!("ok"));
                });
            }

            #[test]
            fn async_send_default_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let fut = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            true,
                            Duration::from_secs(5),
                            None,
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    match rx.try_recv() {
                        Ok(Message::Text(t)) => assert_eq!(t, "msg"),
                        _ => panic!("no text"),
                    }
                    {
                        let mut st = conn.state.lock().await;
                        let pr = st.pending_requests.remove("id").unwrap();
                        pr.completion.send(Ok(serde_json::json!(123))).unwrap();
                    }
                    let resp = fut.await.unwrap().unwrap();
                    assert_eq!(resp, serde_json::json!(123));
                });
            }

            #[test]
            fn async_send_failure_removes_pending_entry() {
                // Regression test: when wait_for_reply=true, send() registers a
                // pending_requests entry BEFORE writing to ws_write_tx so that a
                // racing response can never be lost. If the write itself fails
                // afterwards, the pending entry must be removed so it doesn't
                // leak (otherwise close_connection_gracefully would block on it
                // until the 30s drain timeout, and on_message could match a
                // future request that reuses the id).
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let (tx, rx) = unbounded_channel::<Message>();
                    drop(rx);
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let err = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            true,
                            Duration::from_secs(1),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap_err();
                    assert!(matches!(err, WebsocketError::NotConnected));
                    assert!(
                        conn.state.lock().await.pending_requests.is_empty(),
                        "pending_requests must be empty after a failed send"
                    );
                });
            }

            #[test]
            fn async_send_error_if_no_id() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c§");
                    let (tx, _rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let err = common
                        .send(
                            "msg".into(),
                            None,
                            true,
                            Duration::from_secs(1),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap_err();
                    assert!(matches!(err, WebsocketError::NotConnected));
                });
            }

            #[test]
            fn timeout_rejects_async() {
                TOKIO_SHARED_RT.block_on(async {
                    pause();
                    let conn = WebsocketConnection::new("c1");
                    let (tx, _rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let fut = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            true,
                            Duration::from_secs(1),
                            Some(conn.clone()),
                        )
                        .await
                        .unwrap()
                        .unwrap();
                    advance(Duration::from_secs(1)).await;
                    let res = fut.await.unwrap();
                    assert!(res.is_err(), "expected timeout error");
                    assert!(!conn.state.lock().await.pending_requests.contains_key("id"));
                });
            }

            #[test]
            fn async_send_errors_if_no_connection_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let common = WebsocketCommon::new(
                        vec![conn.clone()],
                        WebsocketMode::Single,
                        0,
                        None,
                        None,
                    );
                    let err = common
                        .send(
                            "msg".into(),
                            Some("id".into()),
                            true,
                            Duration::from_secs(1),
                            None,
                        )
                        .await
                        .unwrap_err();
                    assert!(matches!(err, WebsocketError::NotConnected));
                });
            }
        }
    }

    mod websocket_api {
        use super::*;

        mod initialisation {
            use super::*;

            #[test]
            fn new_initializes_common() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("id");
                    let pool = vec![conn.clone()];

                    let sig_gen = SignatureGenerator::new(
                        Some("api_secret".to_string()),
                        None::<PrivateKey>,
                        None::<String>,
                    );

                    let config = ConfigurationWebsocketApi {
                        api_key: Some("api_key".to_string()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 1000,
                        signature_gen: sig_gen,
                        timeout: 500,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };

                    let api = WebsocketApi::new(config, pool.clone());

                    assert_eq!(api.common.connection_pool.len(), 1);
                    assert_eq!(api.common.mode, WebsocketMode::Single);

                    let flag = *api.is_connecting.lock().await;
                    assert!(!flag);
                });
            }
        }

        mod connect {
            use super::*;

            #[test]
            fn connect_when_not_connected_establishes() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("id");
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = None;
                    }
                    let sig = SignatureGenerator::new(
                        Some("api_secret".into()),
                        None::<PrivateKey>,
                        None::<String>,
                    );
                    let cfg = ConfigurationWebsocketApi {
                        api_key: Some("api_key".into()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("ws://doesnotexist:1".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 0,
                        signature_gen: sig,
                        timeout: 10,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let api = WebsocketApi::new(cfg, vec![conn.clone()]);
                    let res = api.clone().connect().await;
                    assert!(!matches!(res, Err(WebsocketError::Timeout)));
                });
            }

            #[test]
            fn already_connected_returns_ok() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("id2");
                    let (tx, _) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let sig = SignatureGenerator::new(
                        Some("api_secret".to_string()),
                        None::<PrivateKey>,
                        None::<String>,
                    );
                    let cfg = ConfigurationWebsocketApi {
                        api_key: Some("api_key".to_string()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("ws://example.com".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 0,
                        signature_gen: sig,
                        timeout: 10,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let api = WebsocketApi::new(cfg, vec![conn.clone()]);
                    let res = api.connect().await;
                    assert!(res.is_ok());
                });
            }

            #[test]
            fn not_connected_returns_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("id1");
                    let sig = SignatureGenerator::new(
                        Some("api_secret".to_string()),
                        None::<PrivateKey>,
                        None::<String>,
                    );
                    let cfg = ConfigurationWebsocketApi {
                        api_key: Some("api_key".to_string()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("ws://127.0.0.1:9".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 0,
                        signature_gen: sig,
                        timeout: 10,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let api = WebsocketApi::new(cfg, vec![conn.clone()]);
                    let res = api.connect().await;
                    assert!(res.is_err());
                });
            }

            #[test]
            fn concurrent_calls_both_error_or_ok() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("id3");
                    let sig = SignatureGenerator::new(
                        Some("api_secret".to_string()),
                        None::<PrivateKey>,
                        None::<String>,
                    );
                    let cfg = ConfigurationWebsocketApi {
                        api_key: Some("api_key".to_string()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("wss://invalid-domain".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 0,
                        signature_gen: sig,
                        timeout: 10,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let api = WebsocketApi::new(cfg, vec![conn.clone()]);
                    let fut1 = tokio::spawn(api.clone().connect());
                    let fut2 = tokio::spawn(api.clone().connect());
                    let r1 = fut1.await.unwrap();
                    let r2 = fut2.await.unwrap();

                    assert!(r1.is_err());
                    assert!(r2.is_err() || r2.is_ok());
                });
            }

            #[test]
            fn pool_failure_is_propagated() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("w");
                    let sig = SignatureGenerator::new(
                        Some("api_secret".to_string()),
                        None::<PrivateKey>,
                        None::<String>,
                    );
                    let cfg = ConfigurationWebsocketApi {
                        api_key: Some("api_key".into()),
                        api_secret: Some("api_secret".to_string()),
                        private_key: None,
                        private_key_passphrase: None,
                        ws_url: Some("ws://doesnotexist:1".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 0,
                        signature_gen: sig,
                        timeout: 10,
                        time_unit: None,
                        auto_session_relogon: false,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let api = WebsocketApi::new(cfg, vec![conn.clone()]);
                    let res = api.clone().connect().await;
                    match res {
                        Err(WebsocketError::Handshake(_) | WebsocketError::Timeout) => {}
                        _ => panic!("expected handshake or timeout error"),
                    }
                });
            }
        }

        mod send_message {
            use super::*;

            #[test]
            fn unsigned_message() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, None, None);
                    let conn = &api.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let mut params = BTreeMap::new();
                            params.insert("foo".into(), Value::String("bar".into()));
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    params,
                                    WebsocketMessageSendOptions {
                                        with_api_key: false,
                                        is_signed: false,
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Single(resp) => resp,
                                SendWebsocketMessageResult::Multiple(_) => {
                                    panic!("expected single response")
                                }
                            }
                        }
                    });

                    let Message::Text(txt) = rx.recv().await.unwrap() else {
                        panic!()
                    };
                    let req: Value = serde_json::from_str(&txt).unwrap();
                    assert_eq!(req["method"], "method");
                    assert_eq!(req["params"]["foo"], "bar");
                    assert!(req["params"].get("apiKey").is_none());
                    assert!(req["params"].get("timestamp").is_none());
                    assert!(req["params"].get("signature").is_none());

                    let id = req["id"].as_str().unwrap().to_string();
                    let mut st = conn.state.lock().await;
                    let pending = st.pending_requests.remove(&id).unwrap();
                    let reply = json!({
                        "id": id,
                        "result": { "x": 42 },
                        "rateLimits": [{ "limit": 7 }]
                    });
                    pending.completion.send(Ok(reply)).unwrap();

                    let resp = fut.await.unwrap();
                    let rate_limits = resp.rate_limits.unwrap_or_default();

                    assert!(rate_limits.is_empty());
                    assert_eq!(resp.raw, json!({"x": 42}));
                });
            }

            #[test]
            fn with_api_key_only() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, None, None);
                    let conn = &api.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let params = BTreeMap::new();
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    params,
                                    WebsocketMessageSendOptions {
                                        with_api_key: true,
                                        is_signed: false,
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Single(resp) => resp,
                                SendWebsocketMessageResult::Multiple(_) => {
                                    panic!("expected single response")
                                }
                            }
                        }
                    });

                    let Message::Text(txt) = rx.recv().await.unwrap() else {
                        panic!()
                    };
                    let req: Value = serde_json::from_str(&txt).unwrap();
                    assert_eq!(req["params"]["apiKey"], "api_key");

                    let id = req["id"].as_str().unwrap().to_string();
                    let mut st = conn.state.lock().await;
                    let pending = st.pending_requests.remove(&id).unwrap();
                    pending
                        .completion
                        .send(Ok(json!({
                            "id": id,
                            "result": {},
                            "rateLimits": []
                        })))
                        .unwrap();

                    let resp = fut.await.unwrap();

                    assert_eq!(resp.raw, json!({}));
                    assert!(st.pending_requests.is_empty());
                });
            }

            #[test]
            fn signed_message_has_timestamp_and_signature() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, None, None);
                    let conn = &api.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let mut params = BTreeMap::new();
                            params.insert("foo".into(), Value::String("bar".into()));
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    params,
                                    WebsocketMessageSendOptions {
                                        with_api_key: true,
                                        is_signed: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Single(resp) => resp,
                                SendWebsocketMessageResult::Multiple(_) => {
                                    panic!("expected single response")
                                }
                            }
                        }
                    });

                    let Message::Text(txt) = rx.recv().await.unwrap() else {
                        panic!()
                    };
                    let req: Value = serde_json::from_str(&txt).unwrap();
                    let p = &req["params"];
                    assert_eq!(p["apiKey"], "api_key");
                    assert!(p["timestamp"].is_number());
                    assert!(p["signature"].is_string());

                    let id = req["id"].as_str().unwrap().to_string();
                    let mut st = conn.state.lock().await;
                    let pending = st.pending_requests.remove(&id).unwrap();
                    pending
                        .completion
                        .send(Ok(json!({
                            "id": id,
                            "result": { "ok": true },
                            "rateLimits": []
                        })))
                        .unwrap();

                    let resp = fut.await.unwrap();
                    assert_eq!(resp.raw, json!({ "ok": true }));
                });
            }

            #[test]
            fn multi_session_logon() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, Some(WebsocketMode::Pool(2)), None);
                    let conn0 = &api.common.connection_pool[0];
                    let conn1 = &api.common.connection_pool[1];

                    let (tx0, mut rx0) = unbounded_channel::<Message>();
                    let (tx1, mut rx1) = unbounded_channel::<Message>();
                    {
                        let mut st0 = conn0.state.lock().await;
                        st0.ws_write_tx = Some(tx0);
                    }
                    {
                        let mut st1 = conn1.state.lock().await;
                        st1.ws_write_tx = Some(tx1);
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let params = BTreeMap::new();
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    params,
                                    WebsocketMessageSendOptions {
                                        is_session_logon: Some(true),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Multiple(v) => v,
                                SendWebsocketMessageResult::Single(_) => {
                                    panic!("expected multiple responses")
                                }
                            }
                        }
                    });

                    let Message::Text(txt0) = rx0.recv().await.unwrap() else {
                        panic!()
                    };
                    let Message::Text(txt1) = rx1.recv().await.unwrap() else {
                        panic!()
                    };
                    let req0: Value = serde_json::from_str(&txt0).unwrap();
                    let req1: Value = serde_json::from_str(&txt1).unwrap();
                    assert_eq!(req0["method"], "method");
                    assert_eq!(req1["method"], "method");
                    let id = req0["id"].as_str().unwrap().to_string();
                    assert_eq!(req1["id"].as_str().unwrap(), &id);

                    {
                        let mut st0 = conn0.state.lock().await;
                        let pending0 = st0.pending_requests.remove(&id).unwrap();
                        pending0
                            .completion
                            .send(Ok(json!({
                                "id": id,
                                "result": { "ok": true },
                                "rateLimits": []
                            })))
                            .unwrap();
                    }
                    {
                        let mut st1 = conn1.state.lock().await;
                        let pending1 = st1.pending_requests.remove(&id).unwrap();
                        pending1
                            .completion
                            .send(Ok(json!({
                                "id": id,
                                "result": { "ok": true },
                                "rateLimits": []
                            })))
                            .unwrap();
                    }

                    let results = fut.await.unwrap();
                    assert_eq!(results.len(), 2);

                    for conn in &api.common.connection_pool {
                        let st = conn.state.lock().await;
                        assert!(st.is_session_logged_on, "should be logged out");
                        assert!(st.session_logon_req.is_some(), "req cleared");

                        // let req = st
                        //     .session_logon_req
                        //     .as_ref()
                        //     .expect("session_logon_req should be Some(_)");
                        // assert_eq!(req.method, "method");
                        // let mut expected = BTreeMap::new();
                        // expected.insert("ok".to_string(), Value::Bool(true));
                        // assert_eq!(
                        //     req.payload, expected,
                        //     "stored payload should be {{ \"ok\": true }}"
                        // );
                        // assert!(
                        //     req.options.is_session_logon.unwrap_or(false),
                        //     "expected options.is_session_logon = true"
                        // );
                    }
                });
            }

            #[test]
            fn multi_session_logout() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, Some(WebsocketMode::Pool(2)), None);

                    for conn in &api.common.connection_pool {
                        let (tx, _rx) = unbounded_channel::<Message>();
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.is_session_logged_on = true;
                        st.session_logon_req = Some(WebsocketSessionLogonReq {
                            method: "method".into(),
                            payload: BTreeMap::new(),
                            options: WebsocketMessageSendOptions::default(),
                        });
                    }

                    let mut rxs = Vec::new();
                    for conn in &api.common.connection_pool {
                        let rx = {
                            let (tx, rx) = unbounded_channel::<Message>();
                            conn.state.lock().await.ws_write_tx = Some(tx);
                            rx
                        };
                        rxs.push(rx);
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    BTreeMap::new(),
                                    WebsocketMessageSendOptions {
                                        is_signed: false,
                                        with_api_key: false,
                                        is_session_logout: Some(true),
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Multiple(v) => v,
                                SendWebsocketMessageResult::Single(_) => panic!("expected multi"),
                            }
                        }
                    });

                    let mut ids = Vec::new();
                    for mut rx in rxs {
                        let Message::Text(txt) = rx.recv().await.unwrap() else {
                            panic!()
                        };
                        let req: Value = serde_json::from_str(&txt).unwrap();
                        assert_eq!(req["method"], "method");
                        ids.push(req["id"].as_str().unwrap().to_string());
                    }

                    assert_eq!(ids[0], ids[1]);

                    for conn in &api.common.connection_pool {
                        let id = &ids[0];
                        let mut st = conn.state.lock().await;
                        let pending = st.pending_requests.remove(id).unwrap();
                        pending
                            .completion
                            .send(Ok(json!({
                                "id": id,
                                "result": {},
                                "rateLimits": []
                            })))
                            .unwrap();
                    }

                    let results = fut.await.unwrap();
                    assert_eq!(results.len(), 2);

                    for conn in &api.common.connection_pool {
                        let st = conn.state.lock().await;
                        assert!(!st.is_session_logged_on, "should be logged out");
                        assert!(st.session_logon_req.is_none(), "req cleared");
                    }
                });
            }

            #[test]
            fn skip_signature_when_logged_on_and_auto_relogon() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, Some(WebsocketMode::Single), None);
                    let conn = &api.common.connection_pool[0];
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(unbounded_channel::<Message>().0);
                        st.is_session_logged_on = true;
                    }

                    let mut rx;
                    {
                        let mut st = conn.state.lock().await;
                        let (tx, new_rx) = unbounded_channel::<Message>();
                        st.ws_write_tx = Some(tx);
                        rx = new_rx;
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    BTreeMap::new(),
                                    WebsocketMessageSendOptions {
                                        is_signed: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Single(resp) => resp,
                                SendWebsocketMessageResult::Multiple(_) => {
                                    panic!("expected single")
                                }
                            }
                        }
                    });

                    let Message::Text(txt) = rx.recv().await.unwrap() else {
                        panic!()
                    };
                    let req: Value = serde_json::from_str(&txt).unwrap();
                    let p = &req["params"];
                    assert!(p.get("timestamp").is_some());
                    assert!(p.get("signature").is_none());

                    let id = req["id"].as_str().unwrap();
                    let mut st = conn.state.lock().await;
                    let pending = st.pending_requests.remove(id).unwrap();
                    pending
                        .completion
                        .send(Ok(json!({
                            "id": id,
                            "result": {},
                            "rateLimits": []
                        })))
                        .unwrap();

                    let resp = fut.await.unwrap();
                    assert_eq!(resp.raw, json!({}));
                });
            }

            #[test]
            fn include_signature_when_logged_on_and_no_auto_relogon() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, Some(WebsocketMode::Single), Some(false));
                    let conn = &api.common.connection_pool[0];
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(unbounded_channel::<Message>().0);
                        st.is_session_logged_on = true;
                    }

                    let mut rx;
                    {
                        let mut st = conn.state.lock().await;
                        let (tx, new_rx) = unbounded_channel::<Message>();
                        st.ws_write_tx = Some(tx);
                        rx = new_rx;
                    }

                    let fut = tokio::spawn({
                        let api = api.clone();
                        async move {
                            let send_res = api
                                .send_message::<Value>(
                                    "method",
                                    BTreeMap::new(),
                                    WebsocketMessageSendOptions {
                                        is_signed: true,
                                        ..Default::default()
                                    },
                                )
                                .await
                                .unwrap();

                            match send_res {
                                SendWebsocketMessageResult::Single(resp) => resp,
                                SendWebsocketMessageResult::Multiple(_) => {
                                    panic!("expected single")
                                }
                            }
                        }
                    });

                    let Message::Text(txt) = rx.recv().await.unwrap() else {
                        panic!()
                    };
                    let req: Value = serde_json::from_str(&txt).unwrap();
                    let p = &req["params"];
                    assert!(p.get("timestamp").is_some());
                    assert!(p.get("signature").is_some());

                    let id = req["id"].as_str().unwrap();
                    let mut st = conn.state.lock().await;
                    let pending = st.pending_requests.remove(id).unwrap();
                    pending
                        .completion
                        .send(Ok(json!({
                            "id": id,
                            "result": {},
                            "rateLimits": []
                        })))
                        .unwrap();

                    let resp = fut.await.unwrap();
                    assert_eq!(resp.raw, json!({}));
                });
            }

            #[test]
            fn error_if_not_connected() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, None, None);
                    let conn = &api.common.connection_pool[0];
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = None;
                    }
                    let params = BTreeMap::new();
                    let err = api
                        .send_message::<Value>(
                            "method",
                            params,
                            WebsocketMessageSendOptions {
                                with_api_key: false,
                                is_signed: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap_err();
                    matches!(err, WebsocketError::NotConnected);
                });
            }
        }

        mod prepare_url {
            use super::*;

            #[test]
            fn no_time_unit() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(None, None, None);
                    let url = "wss://example.com/ws".to_string();
                    assert_eq!(api.prepare_url(&url), url);
                });
            }

            #[test]
            fn appends_time_unit() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(Some(TimeUnit::Millisecond), None, None);
                    let base = "wss://example.com/ws".to_string();
                    let got = api.prepare_url(&base);
                    assert_eq!(got, format!("{base}?timeUnit=millisecond"));
                });
            }

            #[test]
            fn handles_existing_query() {
                TOKIO_SHARED_RT.block_on(async {
                    let api = create_websocket_api(Some(TimeUnit::Microsecond), None, None);
                    let base = "wss://example.com/ws?foo=bar".to_string();
                    let got = api.prepare_url(&base);
                    assert_eq!(got, format!("{base}&timeUnit=microsecond"));
                });
            }
        }

        mod on_open {
            use super::*;

            fn create_websocket_api_and_conn() -> (Arc<WebsocketApi>, Arc<WebsocketConnection>) {
                let sig_gen = SignatureGenerator::new(
                    Some("api_secret".to_string()),
                    None::<_>,
                    None::<String>,
                );
                let config = ConfigurationWebsocketApi {
                    api_key: Some("api_key".to_string()),
                    api_secret: Some("api_secret".to_string()),
                    private_key: None,
                    private_key_passphrase: None,
                    ws_url: Some("wss://example".to_string()),
                    mode: WebsocketMode::Single,
                    reconnect_delay: 0,
                    signature_gen: sig_gen,
                    timeout: 1000,
                    time_unit: None,
                    auto_session_relogon: true,
                    agent: None,
                    user_agent: build_user_agent("product"),
                };
                let conn = WebsocketConnection::new("test-conn");
                let api = WebsocketApi::new(config, vec![conn.clone()]);
                (api, conn)
            }

            #[test]
            fn session_relogon_on_open() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    let req = WebsocketSessionLogonReq {
                        method: "method".into(),
                        payload: {
                            let mut m = BTreeMap::new();
                            m.insert("foo".into(), Value::String("bar".into()));
                            m
                        },
                        options: WebsocketMessageSendOptions {
                            with_api_key: true,
                            is_signed: true,
                            is_session_logon: Some(true),
                            ..Default::default()
                        },
                    };

                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.session_logon_req = Some(req.clone());
                        st.is_session_logged_on = false;
                        st.ws_write_tx = Some(tx);
                    }

                    api.on_open("wss://example".to_string(), conn.clone()).await;

                    let Message::Text(raw) = rx.recv().await.unwrap() else {
                        panic!("expected a Text message");
                    };
                    let msg: Value = serde_json::from_str(&raw).unwrap();
                    assert_eq!(msg["method"], "method");
                    assert_eq!(msg["params"]["foo"], "bar");

                    let id = msg["id"].as_str().unwrap().to_string();
                    {
                        let mut st = conn.state.lock().await;
                        let pending = st.pending_requests.remove(&id).expect("pending request");
                        pending
                            .completion
                            .send(Ok(json!({
                                "id": id,
                                "result": {},
                                "rateLimits": []
                            })))
                            .unwrap();
                    }

                    sleep(Duration::from_millis(10)).await;

                    let st = conn.state.lock().await;
                    assert!(st.is_session_logged_on, "should now be logged on");
                });
            }

            #[test]
            fn no_relogon_if_already_logged_on() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    let req = WebsocketSessionLogonReq {
                        method: "method".into(),
                        payload: BTreeMap::new(),
                        options: WebsocketMessageSendOptions {
                            is_session_logon: Some(true),
                            ..Default::default()
                        },
                    };

                    let (tx, mut rx) = unbounded_channel::<Message>();
                    {
                        let mut st = conn.state.lock().await;
                        st.session_logon_req = Some(req);
                        st.is_session_logged_on = true;
                        st.ws_write_tx = Some(tx);
                    }

                    api.on_open("wss://example".to_string(), conn.clone()).await;

                    assert!(rx.try_recv().is_err(), "no re‐logon when already on");

                    let st = conn.state.lock().await;
                    assert!(st.is_session_logged_on);
                });
            }

            #[test]
            fn session_relogon_fails_gracefully() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    let req = WebsocketSessionLogonReq {
                        method: "method".into(),
                        payload: {
                            let mut m = BTreeMap::new();
                            m.insert("x".into(), Value::Number(1.into()));
                            m
                        },
                        options: WebsocketMessageSendOptions {
                            is_session_logon: Some(true),
                            ..Default::default()
                        },
                    };
                    {
                        let mut st = conn.state.lock().await;
                        st.session_logon_req = Some(req);
                        st.is_session_logged_on = false;
                        st.ws_write_tx = None;
                    }

                    api.on_open("wss://example".into(), conn.clone()).await;

                    let st = conn.state.lock().await;
                    assert!(
                        !st.is_session_logged_on,
                        "should remain logged‐off on failure"
                    );
                });
            }

            #[test]
            fn session_relogon_noop_when_no_req() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    {
                        let mut st = conn.state.lock().await;
                        st.session_logon_req = None;
                        st.is_session_logged_on = false;
                        st.ws_write_tx = Some(unbounded_channel::<Message>().0);
                    }

                    api.on_open("wss://example".into(), conn.clone()).await;

                    let st = conn.state.lock().await;
                    assert!(!st.is_session_logged_on, "still logged‐off");
                });
            }
        }

        mod on_message {
            use super::*;

            fn create_websocket_api_and_conn() -> (Arc<WebsocketApi>, Arc<WebsocketConnection>) {
                let sig_gen = SignatureGenerator::new(
                    Some("api_secret".to_string()),
                    None::<_>,
                    None::<String>,
                );
                let config = ConfigurationWebsocketApi {
                    api_key: Some("api_key".to_string()),
                    api_secret: Some("api_secret".to_string()),
                    private_key: None,
                    private_key_passphrase: None,
                    ws_url: Some("wss://example".to_string()),
                    mode: WebsocketMode::Single,
                    reconnect_delay: 0,
                    signature_gen: sig_gen,
                    timeout: 1000,
                    time_unit: None,
                    auto_session_relogon: false,
                    agent: None,
                    user_agent: build_user_agent("product"),
                };
                let conn = WebsocketConnection::new("test");
                let api = WebsocketApi::new(config, vec![conn.clone()]);
                (api, conn)
            }

            #[test]
            fn resolves_pending_and_removes_request() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests
                            .insert("id1".to_string(), PendingRequest { completion: tx });
                    }
                    let msg = json!({"id":"id1","status":200,"foo":"bar"});
                    api.on_message(msg.to_string(), conn.clone()).await;
                    let got = rx.await.unwrap().unwrap();
                    assert_eq!(got, msg);
                    let st = conn.state.lock().await;
                    assert!(!st.pending_requests.contains_key("id1"));
                });
            }

            #[test]
            fn uses_result_when_present() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests
                            .insert("id1".to_string(), PendingRequest { completion: tx });
                    }
                    let msg = json!({
                        "id": "id1",
                        "status": 200,
                        "response": [1,2],
                        "result": {"a":1}
                    });
                    api.on_message(msg.to_string(), conn.clone()).await;
                    let got = rx.await.unwrap().unwrap();
                    assert_eq!(got.get("result").unwrap(), &json!({"a":1}));
                });
            }

            #[test]
            fn uses_response_when_no_result() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests
                            .insert("id1".to_string(), PendingRequest { completion: tx });
                    }
                    let msg = json!({
                        "id": "id1",
                        "status": 200,
                        "response": ["ok"]
                    });
                    api.on_message(msg.to_string(), conn.clone()).await;
                    let got = rx.await.unwrap().unwrap();
                    assert_eq!(got.get("response").unwrap(), &json!(["ok"]));
                });
            }

            #[test]
            fn errors_for_status_ge_400() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests
                            .insert("bad".to_string(), PendingRequest { completion: tx });
                    }
                    let err_obj = json!({"code":123,"msg":"oops"});
                    let msg = json!({"id":"bad","status":500,"error":err_obj});
                    api.on_message(msg.to_string(), conn.clone()).await;
                    match rx.await.unwrap() {
                        Err(WebsocketError::ResponseError { code, message }) => {
                            assert_eq!(code, 123);
                            assert_eq!(message, "oops");
                        }
                        other => panic!("expected ResponseError, got {other:?}"),
                    }
                    let st = conn.state.lock().await;
                    assert!(!st.pending_requests.contains_key("bad"));
                });
            }

            #[test]
            fn ignores_unknown_id() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let msg = json!({"id":"nope","status":200});
                    api.on_message(msg.to_string(), conn.clone()).await;
                    let st = conn.state.lock().await;
                    assert!(st.pending_requests.is_empty());
                });
            }

            #[test]
            fn parse_error_ignored() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    api.on_message("not json".to_string(), conn.clone()).await;
                    let st = conn.state.lock().await;
                    assert!(st.pending_requests.is_empty());
                });
            }

            #[test]
            fn error_status_sends_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    let (tx, rx) = oneshot::channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests
                            .insert("err".to_string(), PendingRequest { completion: tx });
                    }
                    let msg = json!({
                        "id": "err",
                        "status": 500,
                        "error": { "code": 42, "msg": "Bad!" }
                    });
                    api.on_message(msg.to_string(), conn.clone()).await;
                    match rx.await.unwrap() {
                        Err(WebsocketError::ResponseError { code, message }) => {
                            assert_eq!(code, 42);
                            assert_eq!(message, "Bad!");
                        }
                        other => panic!("expected ResponseError, got {other:?}"),
                    }
                });
            }

            #[test]
            fn unknown_id_logs_warning_and_leaves_pending() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests.insert(
                            "keep".to_string(),
                            PendingRequest {
                                completion: oneshot::channel().0,
                            },
                        );
                    }
                    api.on_message(
                        json!({ "id": "foo", "status": 200, "result": 1 }).to_string(),
                        conn.clone(),
                    )
                    .await;
                    let st = conn.state.lock().await;
                    assert!(st.pending_requests.contains_key("keep"));
                });
            }

            #[test]
            fn server_shutdown_enqueues_reconnect() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    let msg = json!({
                        "event": { "e": "serverShutdown" }
                    });

                    api.on_message(msg.to_string(), conn.clone()).await;

                    let st = conn.state.lock().await;
                    assert!(st.renewal_pending);
                });
            }

            #[test]
            fn server_shutdown_ignored_if_renewal_pending() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    {
                        let mut st = conn.state.lock().await;
                        st.renewal_pending = true;
                    }

                    let msg = json!({
                        "event": { "e": "serverShutdown" }
                    });

                    api.on_message(msg.to_string(), conn.clone()).await;

                    let st = conn.state.lock().await;

                    assert!(st.renewal_pending);
                });
            }

            #[test]
            fn server_shutdown_ignored_if_close_initiated() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    {
                        let mut st = conn.state.lock().await;
                        st.close_initiated = true;
                    }

                    let msg = json!({
                        "event": { "e": "serverShutdown" }
                    });

                    api.on_message(msg.to_string(), conn.clone()).await;

                    let st = conn.state.lock().await;

                    assert!(!st.renewal_pending);
                });
            }

            #[test]
            fn server_shutdown_does_not_touch_pending_requests() {
                TOKIO_SHARED_RT.block_on(async {
                    let (api, conn) = create_websocket_api_and_conn();

                    {
                        let mut st = conn.state.lock().await;
                        st.pending_requests.insert(
                            "keep".to_string(),
                            PendingRequest {
                                completion: oneshot::channel().0,
                            },
                        );
                    }

                    let msg = json!({
                        "event": { "e": "serverShutdown" }
                    });

                    api.on_message(msg.to_string(), conn.clone()).await;

                    let st = conn.state.lock().await;

                    assert!(st.pending_requests.contains_key("keep"));
                    assert!(st.renewal_pending);
                });
            }
        }
    }

    mod websocket_streams {
        use super::*;

        mod initialisation {
            use super::*;

            #[test]
            fn new_initializes_fields() {
                TOKIO_SHARED_RT.block_on(async {
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Pool(2),
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");
                    let api = WebsocketStreams::new(
                        config.clone(),
                        vec![conn1.clone(), conn2.clone()],
                        vec![],
                    );

                    assert_eq!(api.common.connection_pool.len(), 2);
                    assert!(Arc::ptr_eq(&api.common.connection_pool[0], &conn1));
                    assert!(Arc::ptr_eq(&api.common.connection_pool[1], &conn2));
                    assert_eq!(api.configuration.ws_url, Some("wss://example".to_string()));
                    let flag = api.is_connecting.lock().await;
                    assert!(!*flag);
                });
            }

            #[test]
            fn new_expands_pool_when_url_paths_present() {
                TOKIO_SHARED_RT.block_on(async {
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Pool(2),
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };

                    let conn1 = WebsocketConnection::new("c1");
                    let conn2 = WebsocketConnection::new("c2");

                    let api = WebsocketStreams::new(
                        config,
                        vec![conn1.clone(), conn2.clone()],
                        vec!["path1".to_string(), "path2".to_string()],
                    );

                    assert_eq!(api.common.connection_pool.len(), 4);
                    assert!(Arc::ptr_eq(&api.common.connection_pool[0], &conn1));
                    assert!(Arc::ptr_eq(&api.common.connection_pool[1], &conn2));
                });
            }

            #[test]
            fn new_does_not_expand_pool_when_already_sized_for_url_paths() {
                TOKIO_SHARED_RT.block_on(async {
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Pool(2),
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };

                    let conns = vec![
                        WebsocketConnection::new("c1"),
                        WebsocketConnection::new("c2"),
                        WebsocketConnection::new("c3"),
                        WebsocketConnection::new("c4"),
                    ];

                    let api = WebsocketStreams::new(
                        config,
                        conns.clone(),
                        vec!["path1".to_string(), "path2".to_string()],
                    );

                    assert_eq!(api.common.connection_pool.len(), 4);
                    for (i, c) in conns.iter().enumerate() {
                        assert!(Arc::ptr_eq(&api.common.connection_pool[i], c));
                    }
                });
            }
        }

        mod connect {
            use super::*;

            #[test]
            fn establishes_successfully() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let port = listener.local_addr().unwrap().port();

                    tokio::spawn(async move {
                        for _ in 0..2 {
                            if let Ok((stream, _)) = listener.accept().await {
                                let mut ws = accept_async(stream).await.unwrap();
                                ws.close(None).await.ok();
                            }
                        }
                    });

                    let create_websocket_streams = |ws_url: &str| {
                        let c1 = WebsocketConnection::new("c1");
                        let c2 = WebsocketConnection::new("c2");
                        let config = ConfigurationWebsocketStreams {
                            ws_url: Some(ws_url.to_string()),
                            mode: WebsocketMode::Pool(2),
                            reconnect_delay: 500,
                            time_unit: None,
                            raw_frame_observer: None,
                            stream_subscription_observer: None,
                            agent: None,
                            user_agent: build_user_agent("product"),
                        };
                        WebsocketStreams::new(config, vec![c1, c2], vec![])
                    };

                    let url = format!("ws://127.0.0.1:{port}");
                    let ws = create_websocket_streams(&url);

                    let res = ws.connect(vec!["stream1".into()]).await;
                    assert!(res.is_ok());
                });
            }

            #[test]
            fn establishes_successfully_with_url_paths() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    tokio::spawn(async move {
                        for _ in 0..4 {
                            if let Ok((stream, _)) = listener.accept().await {
                                let mut ws = accept_async(stream).await.unwrap();
                                ws.close(None).await.ok();
                            }
                        }
                    });

                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some(format!("ws://{}", addr)),
                        mode: WebsocketMode::Pool(2),
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };

                    let ws = WebsocketStreams::new(
                        config,
                        vec![],
                        vec!["path1".to_string(), "path2".to_string()],
                    );

                    let res = ws.clone().connect(vec!["stream1".into()]).await;
                    assert!(res.is_ok());
                    assert_eq!(ws.common.connection_pool.len(), 4);
                });
            }

            #[test]
            fn connect_sets_url_path_on_connections_when_url_paths_present() {
                TOKIO_SHARED_RT.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let addr = listener.local_addr().unwrap();

                    tokio::spawn(async move {
                        for _ in 0..4 {
                            if let Ok((stream, _)) = listener.accept().await {
                                let mut ws = accept_async(stream).await.unwrap();
                                ws.close(None).await.ok();
                            }
                        }
                    });

                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some(format!("ws://{}", addr)),
                        mode: WebsocketMode::Pool(2),
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };

                    let ws = WebsocketStreams::new(
                        config,
                        vec![],
                        vec!["path1".to_string(), "path2".to_string()],
                    );

                    ws.clone().connect(vec!["stream1".into()]).await.unwrap();

                    let pool_size = ws.configuration.mode.pool_size();

                    for (i, conn) in ws.common.connection_pool.iter().enumerate() {
                        let expected = if i < pool_size { "path1" } else { "path2" };
                        let st = conn.state.lock().await;
                        assert_eq!(st.url_path.as_deref(), Some(expected));
                    }
                });
            }

            #[test]
            fn refused_returns_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(Some("ws://127.0.0.1:9"), None, None);
                    let res = ws.connect(vec!["stream1".into()]).await;
                    assert!(res.is_err());
                });
            }

            #[test]
            fn invalid_url_returns_error() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(Some("not-a-url"), None, None);
                    let res = ws.connect(vec!["s".into()]).await;
                    assert!(res.is_err());
                });
            }
        }

        mod disconnect {
            use super::*;

            #[test]
            fn disconnect_clears_state_and_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = &ws.common.connection_pool[0];
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert("s1".to_string(), Vec::new());
                        state.pending_subscriptions.push_back("s2".to_string());
                    }
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("s3".to_string(), Arc::clone(conn));
                    }

                    let res = ws.disconnect().await;
                    assert!(res.is_ok());

                    let state = conn.state.lock().await;
                    assert!(state.stream_callbacks.is_empty());
                    assert!(state.pending_subscriptions.is_empty());

                    let map = ws.connection_streams.lock().await;
                    assert!(map.is_empty());
                });
            }
        }

        mod subscribe {
            use super::*;

            #[test]
            fn empty_list_does_nothing() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    ws.clone().subscribe(Vec::new(), None, None).await;
                    let map = ws.connection_streams.lock().await;
                    assert!(map.is_empty());
                });
            }

            #[test]
            fn queue_when_not_ready() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    ws.clone().subscribe(vec!["s1".into()], None, None).await;
                    let state = conn.state.lock().await;
                    let pending: Vec<String> =
                        state.pending_subscriptions.iter().cloned().collect();
                    assert_eq!(pending, vec!["s1".to_string()]);
                });
            }

            #[test]
            fn only_one_subscription_per_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    ws.clone().subscribe(vec!["s1".into()], None, None).await;
                    ws.clone().subscribe(vec!["s1".into()], None, None).await;
                    let state = conn.state.lock().await;
                    let pending: Vec<String> =
                        state.pending_subscriptions.iter().cloned().collect();
                    assert_eq!(pending, vec!["s1".to_string()]);
                });
            }

            #[test]
            fn multiple_streams_assigned() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    ws.clone()
                        .subscribe(vec!["s1".into(), "s2".into()], None, None)
                        .await;
                    let map = ws.connection_streams.lock().await;
                    assert!(map.contains_key("s1"));
                    assert!(map.contains_key("s2"));
                });
            }

            #[test]
            fn existing_stream_not_reassigned() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    ws.clone().subscribe(vec!["s1".into()], None, None).await;
                    let first_id = {
                        let map = ws.connection_streams.lock().await;
                        map.get("s1").unwrap().id.clone()
                    };
                    ws.clone()
                        .subscribe(vec!["s1".into(), "s2".into()], None, None)
                        .await;
                    let map = ws.connection_streams.lock().await;
                    let second_id = map.get("s1").unwrap().id.clone();
                    assert_eq!(first_id, second_id);
                    assert!(map.contains_key("s2"));
                });
            }

            #[test]
            fn queue_when_not_ready_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = None;
                        st.url_path = Some("path1".to_string());
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    ws.clone()
                        .subscribe(vec!["s1".into()], None, Some("path1"))
                        .await;

                    let state = conn.state.lock().await;
                    let pending: Vec<String> =
                        state.pending_subscriptions.iter().cloned().collect();
                    assert_eq!(pending, vec!["s1".to_string()]);
                });
            }

            #[test]
            fn only_one_subscription_per_stream_per_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = None;
                        st.url_path = Some("path1".to_string());
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    ws.clone()
                        .subscribe(vec!["s1".into()], None, Some("path1"))
                        .await;
                    ws.clone()
                        .subscribe(vec!["s1".into()], None, Some("path1"))
                        .await;

                    let state = conn.state.lock().await;
                    let pending: Vec<String> =
                        state.pending_subscriptions.iter().cloned().collect();
                    assert_eq!(pending, vec!["s1".to_string()]);
                });
            }

            #[test]
            fn same_stream_can_be_subscribed_on_different_url_paths() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn1 = ws.common.connection_pool[0].clone();
                    let conn2 = ws.common.connection_pool[1].clone();

                    {
                        let mut st1 = conn1.state.lock().await;
                        st1.ws_write_tx = None;
                        st1.url_path = Some("path1".to_string());
                        st1.reconnection_pending = false;
                        st1.close_initiated = false;
                    }
                    {
                        let mut st2 = conn2.state.lock().await;
                        st2.ws_write_tx = None;
                        st2.url_path = Some("path2".to_string());
                        st2.reconnection_pending = false;
                        st2.close_initiated = false;
                    }

                    ws.clone()
                        .subscribe(vec!["s1".into()], None, Some("path1"))
                        .await;
                    ws.clone()
                        .subscribe(vec!["s1".into()], None, Some("path2"))
                        .await;

                    let map = ws.connection_streams.lock().await;
                    assert!(map.contains_key("path1::s1"));
                    assert!(map.contains_key("path2::s1"));
                });
            }
        }

        mod unsubscribe {
            use super::*;

            #[test]
            fn removes_stream_with_no_callbacks() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();

                    {
                        let (tx, _rx) = unbounded_channel::<Message>();
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("s1".to_string(), conn.clone());
                    }
                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks.insert("s1".to_string(), Vec::new());
                    }

                    ws.unsubscribe(vec!["s1".to_string()], None, None).await;

                    assert!(!ws.connection_streams.lock().await.contains_key("s1"));
                    assert!(!conn.state.lock().await.stream_callbacks.contains_key("s1"));
                });
            }

            #[test]
            fn preserves_stream_with_callbacks() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[1].clone();

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("s2".to_string(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state
                            .stream_callbacks
                            .insert("s2".to_string(), vec![Arc::new(|_: &Value| {})]);
                    }

                    ws.unsubscribe(vec!["s2".to_string()], None, None).await;

                    assert!(ws.connection_streams.lock().await.contains_key("s2"));
                    assert!(conn.state.lock().await.stream_callbacks.contains_key("s2"));
                });
            }

            #[test]
            fn does_not_send_if_callbacks_exist() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("s1".to_string(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert(
                            "s1".to_string(),
                            vec![Arc::new(|_: &Value| {}), Arc::new(|_: &Value| {})],
                        );
                    }
                    ws.unsubscribe(vec!["s1".into()], None, None).await;
                    assert!(ws.connection_streams.lock().await.contains_key("s1"));
                    assert!(conn.state.lock().await.stream_callbacks.contains_key("s1"));
                });
            }

            #[test]
            fn warns_if_not_associated() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    ws.unsubscribe(vec!["nope".into()], None, None).await;
                });
            }

            #[test]
            fn empty_list_does_nothing() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let before = ws.connection_streams.lock().await.len();
                    ws.unsubscribe(Vec::<String>::new(), None, None).await;
                    let after = ws.connection_streams.lock().await.len();
                    assert_eq!(before, after);
                });
            }

            #[test]
            fn invalid_custom_id_falls_back() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("foo".to_string(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        let (tx, _rx) = unbounded_channel();
                        state.ws_write_tx = Some(tx);
                        state.stream_callbacks.insert("foo".to_string(), Vec::new());
                    }
                    ws.unsubscribe(
                        vec!["foo".into()],
                        Some(StreamId::Str("bad-id".into())),
                        None,
                    )
                    .await;
                    assert!(!ws.connection_streams.lock().await.contains_key("foo"));
                });
            }

            #[test]
            fn removes_disconnected_pending_subscription_without_sending() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    let stream = "old-listen-key".to_string();
                    let keep_stream = "keep-key".to_string();
                    let url_path = "private";
                    let key = ws.stream_key(&stream, Some(url_path));
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert(key.clone(), conn.clone());
                    }
                    let mut rx = {
                        let (tx, rx) = unbounded_channel();
                        let mut state = conn.state.lock().await;
                        state.ws_write_tx = Some(tx);
                        state.reconnection_pending = true;
                        state.url_path = Some(url_path.to_string());
                        state.stream_callbacks.insert(key.clone(), Vec::new());
                        state.pending_subscriptions.push_back(stream.clone());
                        state.pending_subscriptions.push_back(keep_stream.clone());
                        rx
                    };

                    ws.unsubscribe(vec![stream.clone()], None, Some(url_path))
                        .await;

                    assert!(!ws.connection_streams.lock().await.contains_key(&key));
                    let state = conn.state.lock().await;
                    assert!(!state.stream_callbacks.contains_key(&key));
                    assert!(!state.pending_subscriptions.contains(&stream));
                    assert!(state.pending_subscriptions.contains(&keep_stream));
                    drop(state);
                    assert!(timeout(Duration::from_millis(20), rx.recv()).await.is_err());
                    let reconnect_url = ws
                        .get_reconnect_url(
                            "wss://example/private/stream?streams=old-listen-key".to_string(),
                            conn,
                        )
                        .await;
                    assert!(!reconnect_url.contains("old-listen-key"));
                });
            }

            #[test]
            fn removes_stream_with_no_callbacks_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();

                    {
                        let (tx, _rx) = unbounded_channel::<Message>();
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::s1".to_string(), conn.clone());
                    }
                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks
                            .insert("path1::s1".to_string(), Vec::new());
                    }

                    ws.unsubscribe(vec!["s1".to_string()], None, Some("path1"))
                        .await;

                    assert!(!ws.connection_streams.lock().await.contains_key("path1::s1"));
                    assert!(
                        !conn
                            .state
                            .lock()
                            .await
                            .stream_callbacks
                            .contains_key("path1::s1")
                    );
                });
            }

            #[test]
            fn preserves_stream_with_callbacks_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();

                    {
                        let (tx, _rx) = unbounded_channel::<Message>();
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::s2".to_string(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state
                            .stream_callbacks
                            .insert("path1::s2".to_string(), vec![Arc::new(|_: &Value| {})]);
                    }

                    ws.unsubscribe(vec!["s2".to_string()], None, Some("path1"))
                        .await;

                    assert!(ws.connection_streams.lock().await.contains_key("path1::s2"));
                    assert!(
                        conn.state
                            .lock()
                            .await
                            .stream_callbacks
                            .contains_key("path1::s2")
                    );
                });
            }

            #[test]
            fn url_path_mismatch_does_not_remove_other_path_subscription() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();

                    {
                        let (tx, _rx) = unbounded_channel::<Message>();
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::s1".to_string(), conn.clone());
                    }
                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks
                            .insert("path1::s1".to_string(), Vec::new());
                    }

                    ws.unsubscribe(vec!["s1".to_string()], None, Some("path2"))
                        .await;

                    assert!(ws.connection_streams.lock().await.contains_key("path1::s1"));
                    assert!(
                        conn.state
                            .lock()
                            .await
                            .stream_callbacks
                            .contains_key("path1::s1")
                    );
                });
            }
        }

        mod is_subscribed {
            use super::*;

            #[test]
            fn returns_false_when_not_subscribed() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    assert!(!ws.is_subscribed("unknown").await);
                });
            }

            #[test]
            fn returns_true_when_subscribed() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("stream1".to_string(), conn);
                    }
                    assert!(ws.is_subscribed("stream1").await);
                });
            }

            #[test]
            fn returns_true_when_subscribed_with_url_path_key() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::stream1".to_string(), conn);
                    }
                    assert!(ws.is_subscribed("stream1").await);
                });
            }

            #[test]
            fn returns_true_when_same_stream_subscribed_on_multiple_paths() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn1 = ws.common.connection_pool[0].clone();
                    let conn2 = ws.common.connection_pool[1].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::stream1".to_string(), conn1);
                        map.insert("path2::stream1".to_string(), conn2);
                    }
                    assert!(ws.is_subscribed("stream1").await);
                });
            }

            #[test]
            fn returns_false_when_only_similar_suffix_exists() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::stream10".to_string(), conn);
                    }
                    assert!(!ws.is_subscribed("stream1").await);
                });
            }
        }

        mod stream_key {
            use super::*;

            #[test]
            fn stream_key_without_url_path_returns_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    assert_eq!(ws.stream_key("s1", None), "s1");
                });
            }

            #[test]
            fn stream_key_with_empty_url_path_returns_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    assert_eq!(ws.stream_key("s1", Some("")), "s1");
                });
            }

            #[test]
            fn stream_key_with_url_path_prefixes_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    assert_eq!(ws.stream_key("s1", Some("path1")), "path1::s1");
                });
            }

            #[test]
            fn stream_key_distinguishes_paths() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    assert_eq!(ws.stream_key("s1", Some("path1")), "path1::s1");
                    assert_eq!(ws.stream_key("s1", Some("path2")), "path2::s1");
                });
            }
        }

        mod prepare_url {
            use super::*;

            #[test]
            fn private_json_control_url_never_contains_listen_key() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws =
                        create_websocket_streams(Some("wss://fstream.binance.com"), None, None);
                    let listen_key = "private-listen-key-must-not-leak".to_string();
                    let url = ws.prepare_url(&[listen_key.clone()], Some("private"));
                    assert_eq!(url, "wss://fstream.binance.com/private/stream");
                    assert!(!url.contains(&listen_key));
                    assert!(!url.contains("streams="));
                    assert!(!url.contains("listenKey="));
                });
            }

            #[test]
            fn empty_routed_json_control_uses_query_free_exact_route() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws =
                        create_websocket_streams(Some("wss://fstream.binance.com"), None, None);
                    assert_eq!(
                        ws.prepare_url(&[], Some("public")),
                        "wss://fstream.binance.com/public/stream"
                    );
                    assert_eq!(
                        ws.prepare_url(&[], Some("market")),
                        "wss://fstream.binance.com/market/stream"
                    );
                });
            }

            #[test]
            fn without_time_unit_returns_base_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![
                        WebsocketConnection::new("c1"),
                        WebsocketConnection::new("c2"),
                    ];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let url = ws.prepare_url(&["s1".into(), "s2".into()], None);
                    assert_eq!(url, "wss://example/stream?streams=s1/s2");
                });
            }

            #[test]
            fn with_time_unit_appends_parameter() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: Some(TimeUnit::Millisecond),
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let url = ws.prepare_url(&["a".into()], None);
                    assert_eq!(url, "wss://example/stream?streams=a&timeUnit=millisecond");
                });
            }

            #[test]
            fn multiple_streams_and_time_unit() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: Some(TimeUnit::Microsecond),
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let url = ws.prepare_url(&["x".into(), "y".into(), "z".into()], None);
                    assert_eq!(
                        url,
                        "wss://example/stream?streams=x/y/z&timeUnit=microsecond"
                    );
                });
            }

            #[test]
            fn with_url_path_prefixes_base_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let url = ws.prepare_url(["s1".into()].as_ref(), Some("path1"));
                    assert_eq!(url, "wss://example/path1/stream?streams=s1");
                });
            }

            #[test]
            fn with_url_path_and_time_unit_appends_parameter() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: Some(TimeUnit::Millisecond),
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let url = ws.prepare_url(["a".into()].as_ref(), Some("path1"));
                    assert_eq!(
                        url,
                        "wss://example/path1/stream?streams=a&timeUnit=millisecond"
                    );
                });
            }

            #[test]
            fn url_path_distinguishes_urls_for_same_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let conns = vec![WebsocketConnection::new("c1")];
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://example".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 100,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("product"),
                    };
                    let ws = WebsocketStreams::new(config, conns, vec![]);
                    let u1 = ws.prepare_url(["s1".into()].as_ref(), Some("path1"));
                    let u2 = ws.prepare_url(["s1".into()].as_ref(), Some("path2"));
                    assert_eq!(u1, "wss://example/path1/stream?streams=s1");
                    assert_eq!(u2, "wss://example/path2/stream?streams=s1");
                });
            }
        }

        mod handle_stream_assignment {
            use super::*;

            #[test]
            fn assigns_new_streams_to_connections() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let groups = ws
                        .clone()
                        .handle_stream_assignment(vec!["s1".into(), "s2".into()], None)
                        .await;
                    let mut seen_streams = HashSet::new();
                    for (_conn, streams) in &groups {
                        for s in streams {
                            seen_streams.insert(s);
                        }
                    }
                    assert_eq!(
                        seen_streams,
                        ["s1".to_string(), "s2".to_string()].iter().collect()
                    );
                    assert_eq!(groups.len(), 1);
                });
            }

            #[test]
            fn reuses_existing_connection_for_duplicate_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let _ = ws
                        .clone()
                        .handle_stream_assignment(vec!["s1".into()], None)
                        .await;
                    let groups = ws
                        .clone()
                        .handle_stream_assignment(vec!["s1".into(), "s3".into()], None)
                        .await;
                    let mut all_streams = Vec::new();
                    for (_conn, streams) in groups {
                        all_streams.extend(streams);
                    }
                    all_streams.sort();
                    assert_eq!(all_streams, vec!["s1".to_string(), "s3".to_string()]);
                });
            }

            #[test]
            fn empty_stream_list_returns_empty() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let groups = ws.clone().handle_stream_assignment(vec![], None).await;
                    assert!(groups.is_empty());
                });
            }

            #[test]
            fn closed_or_reconnecting_forces_reassignment_of_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);
                    let mut groups = ws
                        .clone()
                        .handle_stream_assignment(vec!["s1".into()], None)
                        .await;
                    let (conn, _) = groups.pop().unwrap();
                    {
                        let mut st = conn.state.lock().await;
                        st.close_initiated = true;
                    }
                    let groups2 = ws
                        .clone()
                        .handle_stream_assignment(vec!["s2".into()], None)
                        .await;
                    assert_eq!(groups2.len(), 1);
                    let (_new_conn, streams) = &groups2[0];
                    assert_eq!(streams, &vec!["s2".to_string()]);
                });
            }

            #[test]
            fn no_available_connections_falls_back_to_one() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, Some(vec![]), None);
                    let assigned = ws.handle_stream_assignment(vec!["foo".into()], None).await;
                    assert_eq!(assigned.len(), 1);
                    let (_conn, streams) = &assigned[0];
                    assert_eq!(streams.as_slice(), &["foo".to_string()]);
                });
            }

            #[test]
            fn single_connection_groups_multiple_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c1");
                    let ws = create_websocket_streams(None, Some(vec![conn.clone()]), None);
                    let assigned = ws
                        .handle_stream_assignment(vec!["s1".into(), "s2".into()], None)
                        .await;
                    assert_eq!(assigned.len(), 1);
                    let (assigned_conn, streams) = &assigned[0];
                    assert!(Arc::ptr_eq(assigned_conn, &conn));
                    assert_eq!(streams.len(), 2);
                    assert!(streams.contains(&"s1".to_string()));
                    assert!(streams.contains(&"s2".to_string()));
                });
            }

            #[test]
            fn reuse_existing_healthy_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c");
                    let ws = create_websocket_streams(None, Some(vec![conn.clone()]), None);
                    let _ = ws.handle_stream_assignment(vec!["s1".into()], None).await;
                    let second = ws.handle_stream_assignment(vec!["s1".into()], None).await;
                    assert_eq!(second.len(), 1);
                    let (assigned_conn, streams) = &second[0];
                    assert!(Arc::ptr_eq(assigned_conn, &conn));
                    assert_eq!(streams.as_slice(), &["s1".to_string()]);
                });
            }

            #[test]
            fn mix_new_and_assigned_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let conn = WebsocketConnection::new("c");
                    let ws = create_websocket_streams(None, Some(vec![conn.clone()]), None);
                    let _ = ws
                        .handle_stream_assignment(vec!["s1".into(), "s2".into()], None)
                        .await;
                    let mixed = ws
                        .handle_stream_assignment(vec!["s2".into(), "s3".into()], None)
                        .await;
                    assert_eq!(mixed.len(), 1);
                    let (assigned_conn, streams) = &mixed[0];
                    assert!(Arc::ptr_eq(assigned_conn, &conn));
                    let mut got = streams.clone();
                    got.sort();
                    assert_eq!(got, vec!["s2".to_string(), "s3".to_string()]);
                });
            }

            #[test]
            fn assigns_streams_with_url_path_keys() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut st = conn.state.lock().await;
                        st.url_path = Some("path1".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    let groups = ws
                        .handle_stream_assignment(vec!["s1".into(), "s2".into()], Some("path1"))
                        .await;

                    let map = ws.connection_streams.lock().await;
                    assert!(map.contains_key("path1::s1"));
                    assert!(map.contains_key("path1::s2"));
                    assert_eq!(groups.len(), 1);

                    let (_assigned_conn, streams) = &groups[0];
                    let mut got = streams.clone();
                    got.sort();
                    assert_eq!(got, vec!["s1".to_string(), "s2".to_string()]);
                });
            }

            #[test]
            fn same_stream_on_different_paths_creates_distinct_keys() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn1 = ws.common.connection_pool[0].clone();
                    let conn2 = ws.common.connection_pool[1].clone();

                    {
                        let mut st = conn1.state.lock().await;
                        st.url_path = Some("path1".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }
                    {
                        let mut st = conn2.state.lock().await;
                        st.url_path = Some("path2".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    let g1 = ws
                        .handle_stream_assignment(vec!["s1".into()], Some("path1"))
                        .await;
                    let g2 = ws
                        .handle_stream_assignment(vec!["s1".into()], Some("path2"))
                        .await;

                    assert_eq!(g1.len(), 1);
                    assert_eq!(g2.len(), 1);

                    let map = ws.connection_streams.lock().await;
                    assert!(map.contains_key("path1::s1"));
                    assert!(map.contains_key("path2::s1"));
                });
            }

            #[test]
            fn reuses_existing_connection_for_same_path_and_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn = ws.common.connection_pool[0].clone();
                    {
                        let mut st = conn.state.lock().await;
                        st.url_path = Some("path1".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    let first = ws
                        .handle_stream_assignment(vec!["s1".into()], Some("path1"))
                        .await;
                    let second = ws
                        .handle_stream_assignment(vec!["s1".into(), "s2".into()], Some("path1"))
                        .await;

                    assert_eq!(first.len(), 1);
                    assert_eq!(second.len(), 1);

                    let map = ws.connection_streams.lock().await;
                    let c1 = map.get("path1::s1").unwrap().clone();
                    let c2 = map.get("path1::s2").unwrap().clone();
                    assert!(Arc::ptr_eq(&c1, &c2));
                });
            }

            #[test]
            fn closed_or_reconnecting_forces_reassignment_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws = create_websocket_streams(None, None, None);

                    let conn1 = ws.common.connection_pool[0].clone();
                    let conn2 = ws.common.connection_pool[1].clone();

                    {
                        let mut st = conn1.state.lock().await;
                        st.url_path = Some("path1".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }
                    {
                        let mut st = conn2.state.lock().await;
                        st.url_path = Some("path1".to_string());
                        st.ws_write_tx = None;
                        st.reconnection_pending = false;
                        st.close_initiated = false;
                    }

                    let _ = ws
                        .handle_stream_assignment(vec!["s1".into()], Some("path1"))
                        .await;

                    {
                        let mut st = conn1.state.lock().await;
                        st.close_initiated = true;
                    }

                    let _ = ws
                        .handle_stream_assignment(vec!["s1".into()], Some("path1"))
                        .await;

                    let map = ws.connection_streams.lock().await;
                    let assigned = map.get("path1::s1").unwrap().clone();
                    assert!(!Arc::ptr_eq(&assigned, &conn1));
                });
            }
        }

        mod send_subscription_payload {
            use super::*;

            #[test]
            fn subscribe_payload_with_custom_id_fallbacks_if_invalid() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let id = Some("badid".to_string());
                    ws.send_subscription_payload(
                        conn,
                        &vec!["s1".to_string()],
                        id.map(StreamId::from),
                    );
                    let msg = rx.recv().await.expect("no message sent");
                    if let Message::Text(txt) = msg {
                        let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
                        assert_eq!(v["method"], "SUBSCRIBE");
                        let id = v["id"].as_str().unwrap();
                        assert_ne!(id, "badid");
                        assert!(Regex::new(r"^[0-9a-fA-F]{32}$").unwrap().is_match(id));
                    } else {
                        panic!("unexpected message: {msg:?}");
                    }
                });
            }

            #[test]
            fn subscribe_payload_with_and_without_custom_string_id() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://unused"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    let id = Some("deadbeefdeadbeefdeadbeefdeadbeef".to_string());
                    ws.send_subscription_payload(
                        conn,
                        &vec!["a".to_string(), "b".to_string()],
                        id.map(StreamId::from),
                    );
                    let msg1 = rx.recv().await.unwrap();
                    ws.send_subscription_payload(conn, &vec!["x".to_string()], None);
                    let msg2 = rx.recv().await.unwrap();

                    if let Message::Text(txt1) = msg1 {
                        let v1: serde_json::Value = serde_json::from_str(&txt1).unwrap();
                        assert_eq!(v1["id"], "deadbeefdeadbeefdeadbeefdeadbeef");
                        assert_eq!(
                            v1["params"].as_array().unwrap(),
                            &vec![serde_json::json!("a"), serde_json::json!("b")]
                        );
                    } else {
                        panic!()
                    }

                    if let Message::Text(txt2) = msg2 {
                        let v2: serde_json::Value = serde_json::from_str(&txt2).unwrap();
                        assert_eq!(v2["method"], "SUBSCRIBE");
                        let params = v2["params"].as_array().unwrap();
                        assert_eq!(params.len(), 1);
                        assert_eq!(params[0], "x");
                        let id2 = v2["id"].as_str().unwrap();
                        assert!(Regex::new(r"^[0-9a-fA-F]{32}$").unwrap().is_match(id2));
                    } else {
                        panic!()
                    }
                });
            }

            #[test]
            fn subscribe_payload_with_and_without_custom_integer_id() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://unused"), None, None);
                    ws.stream_id_is_strictly_number
                        .store(true, Ordering::Relaxed);
                    let conn = &ws.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }

                    let id = Some(123u32);

                    ws.send_subscription_payload(
                        conn,
                        &vec!["a".to_string(), "b".to_string()],
                        id.map(StreamId::from),
                    );
                    let msg1 = rx.recv().await.unwrap();

                    ws.send_subscription_payload(conn, &vec!["x".to_string()], None);
                    let msg2 = rx.recv().await.unwrap();

                    if let Message::Text(txt1) = msg1 {
                        let v1: serde_json::Value = serde_json::from_str(&txt1).unwrap();
                        assert_eq!(v1["method"], "SUBSCRIBE");
                        assert_eq!(v1["id"].as_u64(), Some(123));
                        assert_eq!(
                            v1["params"].as_array().unwrap(),
                            &vec![serde_json::json!("a"), serde_json::json!("b")]
                        );
                    } else {
                        panic!("Expected Message::Text for msg1");
                    }

                    if let Message::Text(txt2) = msg2 {
                        let v2: serde_json::Value = serde_json::from_str(&txt2).unwrap();
                        assert_eq!(v2["method"], "SUBSCRIBE");

                        let params = v2["params"].as_array().unwrap();
                        assert_eq!(params.len(), 1);
                        assert_eq!(params[0], "x");

                        let id2 = v2.get("id").expect("payload should contain id");
                        assert!(
                            id2.is_number(),
                            "expected numeric id in strict-number mode, got: {id2:?}"
                        );
                        let n = id2.as_u64().unwrap();
                        assert!(u32::try_from(n).is_ok(), "id should fit u32, got {n}");
                    } else {
                        panic!("Expected Message::Text for msg2");
                    }
                });
            }
        }

        mod confirmed_subscription {
            use super::*;

            const LISTEN_KEY: &str = "listen-key-that-must-stay-private";

            async fn setup_private_connection(
                generation: u64,
            ) -> (
                Arc<WebsocketStreams>,
                Arc<WebsocketConnection>,
                tokio::sync::mpsc::UnboundedReceiver<Message>,
            ) {
                setup_private_connection_with_observers(generation, None, None).await
            }

            async fn setup_private_connection_with_observers(
                generation: u64,
                raw_frame_observer: Option<RawFrameObserver>,
                stream_subscription_observer: Option<StreamSubscriptionObserver>,
            ) -> (
                Arc<WebsocketStreams>,
                Arc<WebsocketConnection>,
                tokio::sync::mpsc::UnboundedReceiver<Message>,
            ) {
                let config = ConfigurationWebsocketStreams {
                    ws_url: Some("wss://fstream.binance.com".to_string()),
                    mode: WebsocketMode::Single,
                    reconnect_delay: 500,
                    time_unit: None,
                    raw_frame_observer,
                    stream_subscription_observer,
                    agent: None,
                    user_agent: build_user_agent("confirmed-private-test"),
                };
                let ws = WebsocketStreams::new(
                    config,
                    vec![WebsocketConnection::new("private-c1")],
                    vec![],
                );
                let connection = ws.common.connection_pool[0].clone();
                let (writer, receiver) = unbounded_channel();
                connection
                    .session_generation
                    .store(generation, Ordering::Release);
                {
                    let mut state = connection.state.lock().await;
                    state.url_path = Some("private".to_string());
                    state.ws_write_tx = Some(writer);
                    state.writer_session_generation = Some(generation);
                    state.reconnection_pending = false;
                    state.close_initiated = false;
                }
                (ws, connection, receiver)
            }

            fn observe_subscription_events(
                ws: &WebsocketStreams,
            ) -> (
                Subscription,
                tokio::sync::mpsc::UnboundedReceiver<StreamSubscriptionEvent>,
            ) {
                let (sender, receiver) = unbounded_channel();
                let subscription = ws.subscribe_on_stream_subscription_events(move |event| {
                    let _ = sender.send(event);
                });
                (subscription, receiver)
            }

            async fn next_text(
                receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Message>,
            ) -> Value {
                let message = timeout(Duration::from_millis(200), receiver.recv())
                    .await
                    .expect("timed out waiting for subscription payload")
                    .expect("writer channel closed");
                let Message::Text(text) = message else {
                    panic!("expected text payload, got {message:?}");
                };
                serde_json::from_str(&text).expect("valid JSON subscription payload")
            }

            async fn next_event(
                receiver: &mut tokio::sync::mpsc::UnboundedReceiver<StreamSubscriptionEvent>,
            ) -> StreamSubscriptionEvent {
                timeout(Duration::from_millis(200), receiver.recv())
                    .await
                    .expect("timed out waiting for subscription event")
                    .expect("subscription event channel closed")
            }

            #[test]
            fn configuration_builder_accepts_synchronous_subscription_observer() {
                let observer = StreamSubscriptionObserver::new(|_| {});
                let configuration = ConfigurationWebsocketStreams::builder()
                    .stream_subscription_observer(observer)
                    .build()
                    .unwrap();
                assert!(configuration.stream_subscription_observer.is_some());
            }

            #[test]
            fn synchronous_observer_precedes_writer_and_raw_ack() {
                TOKIO_SHARED_RT.block_on(async {
                    let sequence = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let observed_events = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let dispatch_barrier = Arc::new(std::sync::Barrier::new(2));
                    let (dispatch_entered_tx, dispatch_entered_rx) = std::sync::mpsc::channel();

                    let stream_observer = {
                        let sequence = sequence.clone();
                        let observed_events = observed_events.clone();
                        let dispatch_barrier = dispatch_barrier.clone();
                        StreamSubscriptionObserver::new(move |event| {
                            observed_events.lock().unwrap().push(event.clone());
                            match event.outcome {
                                StreamSubscriptionOutcome::Dispatched => {
                                    sequence.lock().unwrap().push("dispatched");
                                    let _ = dispatch_entered_tx.send(());
                                    dispatch_barrier.wait();
                                }
                                StreamSubscriptionOutcome::Acknowledged => {
                                    sequence.lock().unwrap().push("acknowledged");
                                }
                                _ => sequence.lock().unwrap().push("other"),
                            }
                        })
                    };
                    let raw_observer = {
                        let sequence = sequence.clone();
                        RawFrameObserver::new_context(move |context| {
                            assert_eq!(context.path_scope, Some("private"));
                            sequence.lock().unwrap().push("raw");
                        })
                    };
                    let (ws, connection, mut writes) = setup_private_connection_with_observers(
                        14,
                        Some(raw_observer),
                        Some(stream_observer),
                    )
                    .await;
                    let handler: Arc<dyn WebsocketHandler> = ws.clone();
                    connection.set_handler(handler).await;

                    let (result_tx, result_rx) = std::sync::mpsc::channel();
                    let subscribe_thread = {
                        let ws = ws.clone();
                        std::thread::spawn(move || {
                            let runtime = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .unwrap();
                            let result = runtime.block_on(ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                141,
                                Duration::from_millis(500),
                                Some("private"),
                            ));
                            let _ = result_tx.send(result);
                        })
                    };

                    dispatch_entered_rx
                        .recv_timeout(Duration::from_millis(200))
                        .expect("synchronous observer did not enter Dispatched");
                    assert!(
                        writes.try_recv().is_err(),
                        "writer observed payload before synchronous Dispatched returned"
                    );
                    dispatch_barrier.wait();

                    let payload = next_text(&mut writes).await;
                    assert_eq!(payload["id"].as_u64(), Some(141));
                    ws.common
                        .on_text_frame(
                            json!({"result": null, "id": 141}).to_string(),
                            connection.clone(),
                            14,
                            Some("private"),
                        )
                        .await;
                    let ack = result_rx
                        .recv_timeout(Duration::from_millis(500))
                        .expect("confirmed subscriber did not finish")
                        .unwrap();
                    subscribe_thread.join().unwrap();

                    assert_eq!(ack.context.path_scope, StreamSubscriptionScope::Private);
                    assert_eq!(
                        *sequence.lock().unwrap(),
                        vec!["dispatched", "raw", "acknowledged"]
                    );
                    let events = observed_events.lock().unwrap();
                    assert_eq!(events.len(), 2);
                    assert!(events.iter().all(|event| {
                        event.context.path_scope == StreamSubscriptionScope::Private
                    }));
                    assert!(!format!("{events:?}").contains(LISTEN_KEY));
                });
            }

            #[test]
            fn synchronous_observer_panic_is_contained() {
                TOKIO_SHARED_RT.block_on(async {
                    let observer = StreamSubscriptionObserver::new(|_| {
                        panic!("observer failure must stay contained");
                    });
                    let (ws, connection, mut writes) =
                        setup_private_connection_with_observers(15, None, Some(observer)).await;
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                151,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let payload = next_text(&mut writes).await;
                    assert_eq!(payload["id"].as_u64(), Some(151));
                    ws.on_message_with_session(
                        json!({"result": null, "id": 151}).to_string(),
                        connection,
                        15,
                    )
                    .await;
                    let ack = subscribe.await.unwrap().unwrap();
                    assert_eq!(ack.context.path_scope, StreamSubscriptionScope::Private);
                });
            }

            #[test]
            fn exact_unsigned_id_ack_is_bound_to_connection_and_generation() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(7).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let request_id = u32::MAX;
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                request_id,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };

                    let payload = next_text(&mut writes).await;
                    assert_eq!(payload["method"], "SUBSCRIBE");
                    assert_eq!(payload["id"].as_u64(), Some(u64::from(request_id)));
                    assert_eq!(payload["params"], json!([LISTEN_KEY]));
                    let dispatched = next_event(&mut events).await;
                    assert_eq!(dispatched.outcome, StreamSubscriptionOutcome::Dispatched);
                    assert_eq!(dispatched.context.request_id, request_id);
                    assert_eq!(dispatched.context.session_generation, 7);

                    ws.on_message_with_session(
                        json!({"result": null, "id": request_id}).to_string(),
                        connection.clone(),
                        7,
                    )
                    .await;

                    let ack = subscribe.await.unwrap().unwrap();
                    assert_eq!(ack.context.connection_id, connection.id);
                    assert_eq!(ack.context.session_generation, 7);
                    assert_eq!(ack.context.request_id, request_id);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .contains(&format!("private::{LISTEN_KEY}"))
                    );
                    let event = next_event(&mut events).await;
                    assert_eq!(event.context, ack.context);
                    assert_eq!(event.outcome, StreamSubscriptionOutcome::Acknowledged);
                });
            }

            #[test]
            fn correlated_reject_is_redacted_and_releases_unconfirmed_desired_state() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(3).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                17,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    ws.on_message_with_session(
                        json!({
                            "code": 2,
                            "msg": format!("invalid private value {LISTEN_KEY}"),
                            "id": 17
                        })
                        .to_string(),
                        connection.clone(),
                        3,
                    )
                    .await;

                    let error = subscribe.await.unwrap().unwrap_err();
                    assert!(matches!(
                        error,
                        StreamSubscriptionError::Rejected { code: 2, .. }
                    ));
                    assert!(!format!("{error:?}").contains(LISTEN_KEY));
                    assert!(!format!("{error}").contains(LISTEN_KEY));
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .is_empty()
                    );
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Rejected { code: 2 }
                    );
                });
            }

            #[test]
            fn callback_cannot_pin_a_rejected_unconfirmed_reservation() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(4).await;
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                18,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    let key = format!("private::{LISTEN_KEY}");
                    let callback: Arc<dyn Fn(&Value) + Send + Sync> = Arc::new(|_| {});
                    connection
                        .state
                        .lock()
                        .await
                        .stream_callbacks
                        .entry(key.clone())
                        .or_default()
                        .push(callback);

                    ws.on_message_with_session(
                        json!({"code": 2, "id": 18}).to_string(),
                        connection.clone(),
                        4,
                    )
                    .await;
                    assert!(matches!(
                        subscribe.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::Rejected { .. }
                    ));
                    assert!(!ws.connection_streams.lock().await.contains_key(&key));
                    assert!(
                        !connection
                            .state
                            .lock()
                            .await
                            .stream_callbacks
                            .contains_key(&key)
                    );
                });
            }

            #[test]
            fn delivered_reject_wins_over_waiter_cancellation_during_cleanup() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(4).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                19,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );

                    let assignment_lock = ws.connection_streams.lock().await;
                    let reject = {
                        let ws = ws.clone();
                        let connection = connection.clone();
                        tokio::spawn(async move {
                            ws.on_message_with_session(
                                json!({"code": 2, "id": 19}).to_string(),
                                connection,
                                4,
                            )
                            .await;
                        })
                    };
                    assert!(
                        eventually_async(Duration::from_millis(200), || {
                            let connection = connection.clone();
                            async move {
                                !connection
                                    .state
                                    .lock()
                                    .await
                                    .pending_stream_subscriptions
                                    .contains_key(&19)
                            }
                        })
                        .await
                    );
                    subscribe.abort();
                    let _ = subscribe.await;
                    drop(assignment_lock);
                    reject.await.unwrap();

                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Rejected { code: 2 }
                    );
                    assert!(
                        timeout(Duration::from_millis(20), events.recv())
                            .await
                            .is_err()
                    );
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                });
            }

            #[test]
            fn matching_malformed_response_fails_closed() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(5).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                91,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    ws.on_message_with_session(
                        json!({"result": {"unexpected": true}, "id": 91}).to_string(),
                        connection,
                        5,
                    )
                    .await;
                    assert!(matches!(
                        subscribe.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::Protocol { .. }
                    ));
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::ProtocolError
                    );
                });
            }

            #[test]
            fn mixed_or_extra_control_response_fields_fail_closed() {
                TOKIO_SHARED_RT.block_on(async {
                    let cases = [
                        json!({"result": null, "code": 2, "id": 92}),
                        json!({"result": null, "code": "bad", "id": 93}),
                        json!({"result": null, "id": 94, "extra": true}),
                    ];

                    for response in cases {
                        let request_id = response["id"]
                            .as_u64()
                            .and_then(|id| u32::try_from(id).ok())
                            .unwrap();
                        let (ws, connection, mut writes) = setup_private_connection(6).await;
                        let subscribe = {
                            let ws = ws.clone();
                            tokio::spawn(async move {
                                ws.subscribe_one_confirmed(
                                    LISTEN_KEY.to_string(),
                                    request_id,
                                    Duration::from_millis(100),
                                    Some("private"),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        ws.on_message_with_session(response.to_string(), connection, 6)
                            .await;
                        assert!(matches!(
                            subscribe.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::Protocol { .. }
                        ));
                        assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    }
                });
            }

            #[test]
            fn concurrent_same_stream_has_one_dispatch_and_one_owner() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(8).await;
                    let assignment_lock = ws.connection_streams.lock().await;
                    let first = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                101,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let second = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                102,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    tokio::task::yield_now().await;
                    drop(assignment_lock);

                    let payload = next_text(&mut writes).await;
                    let winner_id = payload["id"]
                        .as_u64()
                        .and_then(|id| u32::try_from(id).ok())
                        .unwrap();
                    assert!(winner_id == 101 || winner_id == 102);
                    ws.on_message_with_session(
                        json!({"result": null, "id": winner_id}).to_string(),
                        connection,
                        8,
                    )
                    .await;

                    let results = [first.await.unwrap(), second.await.unwrap()];
                    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
                    assert_eq!(
                        results
                            .iter()
                            .filter(|result| matches!(
                                result,
                                Err(StreamSubscriptionError::AlreadyDesired { .. })
                            ))
                            .count(),
                        1
                    );
                    assert!(writes.try_recv().is_err());
                    assert!(ws.is_subscribed(LISTEN_KEY).await);
                });
            }

            #[test]
            fn request_id_cannot_be_reused_within_one_physical_session() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(10).await;
                    let first = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                121,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    ws.on_message_with_session(
                        json!({"code": 2, "id": 121}).to_string(),
                        connection.clone(),
                        10,
                    )
                    .await;
                    assert!(matches!(
                        first.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::Rejected { .. }
                    ));

                    let retry = ws
                        .clone()
                        .subscribe_one_confirmed(
                            LISTEN_KEY.to_string(),
                            121,
                            Duration::from_millis(200),
                            Some("private"),
                        )
                        .await;
                    assert!(matches!(
                        retry,
                        Err(StreamSubscriptionError::DuplicateRequestId { .. })
                    ));
                    assert!(writes.try_recv().is_err());
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);

                    // A duplicate response from the first request has no new
                    // pending request with the same wire identity to complete.
                    ws.on_message_with_session(
                        json!({"result": null, "id": 121}).to_string(),
                        connection,
                        10,
                    )
                    .await;
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                });
            }

            #[test]
            fn old_generation_pending_id_cannot_be_overwritten_before_open_drain() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let first = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                131,
                                Duration::from_millis(500),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    connection.session_generation.store(2, Ordering::Release);
                    connection.state.lock().await.writer_session_generation = Some(2);

                    let second = ws
                        .clone()
                        .subscribe_one_confirmed(
                            "another-private-key".to_string(),
                            131,
                            Duration::from_millis(100),
                            Some("private"),
                        )
                        .await;
                    assert!(matches!(
                        second,
                        Err(StreamSubscriptionError::DuplicateRequestId { .. })
                    ));
                    assert!(writes.try_recv().is_err());

                    ws.on_open(
                        "wss://fstream.binance.com/private/stream".to_string(),
                        connection,
                    )
                    .await;
                    assert!(matches!(
                        first.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::SessionReplaced { .. }
                    ));
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert!(!ws.is_subscribed("another-private-key").await);
                });
            }

            #[test]
            fn late_timeout_from_cancelled_attempt_cannot_remove_retry_owner() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(9).await;
                    let key = format!("private::{LISTEN_KEY}");
                    let first = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                111,
                                Duration::from_millis(500),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    let first_token = connection
                        .state
                        .lock()
                        .await
                        .unconfirmed_stream_reservations[&key];
                    first.abort();
                    let _ = first.await;
                    assert!(
                        eventually_async(Duration::from_millis(200), || {
                            let ws = ws.clone();
                            async move { !ws.is_subscribed(LISTEN_KEY).await }
                        })
                        .await
                    );

                    let retry = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                112,
                                Duration::from_millis(1_200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    let retry_token = connection
                        .state
                        .lock()
                        .await
                        .unconfirmed_stream_reservations[&key];
                    assert_ne!(first_token, retry_token);

                    sleep(Duration::from_millis(550)).await;
                    assert!(ws.connection_streams.lock().await.contains_key(&key));
                    assert_eq!(
                        connection
                            .state
                            .lock()
                            .await
                            .unconfirmed_stream_reservations[&key],
                        retry_token
                    );

                    ws.on_message_with_session(
                        json!({"result": null, "id": 112}).to_string(),
                        connection.clone(),
                        9,
                    )
                    .await;
                    retry.await.unwrap().unwrap();
                    assert!(ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .contains(&key)
                    );
                });
            }

            #[test]
            fn string_id_does_not_correlate_and_numeric_request_times_out() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(11).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                23,
                                Duration::from_millis(10),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    ws.on_message_with_session(
                        json!({"result": null, "id": "23"}).to_string(),
                        connection,
                        11,
                    )
                    .await;
                    assert!(matches!(
                        subscribe.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::Timeout { .. }
                    ));
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::TimedOut
                    );
                });
            }

            #[test]
            fn reconnect_before_first_ack_does_not_resubscribe_unconfirmed_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                31,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    connection.session_generation.store(2, Ordering::Release);
                    {
                        let mut state = connection.state.lock().await;
                        state.writer_session_generation = Some(2);
                    }

                    // A late response from the old reader cannot acknowledge a
                    // request after the writer slot belongs to generation 2.
                    ws.on_message_with_session(
                        json!({"result": null, "id": 31}).to_string(),
                        connection.clone(),
                        1,
                    )
                    .await;
                    ws.on_open(
                        "wss://fstream.binance.com/private/stream".to_string(),
                        connection.clone(),
                    )
                    .await;

                    assert!(matches!(
                        subscribe.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::SessionReplaced { .. }
                    ));
                    assert!(writes.try_recv().is_err());
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .is_empty()
                    );
                });
            }

            #[test]
            fn confirmed_private_stream_reconnects_without_secret_url_and_resubscribes() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let initial = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                41,
                                Duration::from_millis(100),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    let initial_dispatched = next_event(&mut events).await;
                    assert_eq!(
                        initial_dispatched.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    assert_eq!(initial_dispatched.context.request_id, 41);
                    ws.on_message_with_session(
                        json!({"result": null, "id": 41}).to_string(),
                        connection.clone(),
                        1,
                    )
                    .await;
                    initial.await.unwrap().unwrap();
                    let initial_acknowledged = next_event(&mut events).await;
                    assert_eq!(
                        initial_acknowledged.outcome,
                        StreamSubscriptionOutcome::Acknowledged
                    );
                    assert_eq!(initial_acknowledged.context, initial_dispatched.context);

                    connection.session_generation.store(2, Ordering::Release);
                    {
                        let mut state = connection.state.lock().await;
                        state.writer_session_generation = Some(2);
                    }
                    let reconnect_url = ws
                        .get_reconnect_url("ignored".to_string(), connection.clone())
                        .await;
                    assert_eq!(reconnect_url, "wss://fstream.binance.com/private/stream");
                    assert!(!reconnect_url.contains(LISTEN_KEY));
                    assert!(!reconnect_url.contains("streams="));

                    let reopened = {
                        let ws = ws.clone();
                        let connection = connection.clone();
                        tokio::spawn(async move {
                            ws.on_open(reconnect_url, connection).await;
                        })
                    };
                    let payload = next_text(&mut writes).await;
                    let request_id = payload["id"]
                        .as_u64()
                        .and_then(|id| u32::try_from(id).ok())
                        .expect("reconnect id must be an unsigned u32");
                    assert_eq!(payload["params"], json!([LISTEN_KEY]));
                    let reconnect_dispatched = next_event(&mut events).await;
                    assert_eq!(
                        reconnect_dispatched.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    assert_eq!(reconnect_dispatched.context.request_id, request_id);
                    assert_eq!(reconnect_dispatched.context.session_generation, 2);
                    ws.on_message_with_session(
                        json!({"result": null, "id": request_id}).to_string(),
                        connection.clone(),
                        2,
                    )
                    .await;
                    reopened.await.unwrap();
                    let reconnect_acknowledged = next_event(&mut events).await;
                    assert_eq!(
                        reconnect_acknowledged.outcome,
                        StreamSubscriptionOutcome::Acknowledged
                    );
                    assert_eq!(reconnect_acknowledged.context, reconnect_dispatched.context);
                    assert!(ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .contains(&format!("private::{LISTEN_KEY}"))
                    );
                });
            }

            #[test]
            fn cancelled_initial_waiter_cannot_create_orphan_confirmed_subscription() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                47,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    subscribe.abort();
                    let _ = subscribe.await;

                    ws.on_message_with_session(
                        json!({"result": null, "id": 47}).to_string(),
                        connection.clone(),
                        1,
                    )
                    .await;
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .is_empty()
                    );
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Cancelled
                    );

                    connection.session_generation.store(2, Ordering::Release);
                    {
                        let mut state = connection.state.lock().await;
                        state.writer_session_generation = Some(2);
                    }
                    ws.on_open(
                        "wss://fstream.binance.com/private/stream".to_string(),
                        connection,
                    )
                    .await;
                    assert!(writes.try_recv().is_err());
                    assert!(
                        timeout(Duration::from_millis(20), events.recv())
                            .await
                            .is_err()
                    );
                });
            }

            #[test]
            fn cancelled_attempt_emits_only_one_terminal_after_timeout() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                48,
                                Duration::from_millis(20),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    subscribe.abort();
                    let _ = subscribe.await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Cancelled
                    );
                    {
                        let state = connection.state.lock().await;
                        assert!(!state.pending_stream_subscriptions.contains_key(&48));
                        assert_eq!(state.stream_request_id_generations.get(&48), Some(&1));
                    }

                    // The cancellation event is published only after the old
                    // reservation is gone, so an event-driven retry can claim
                    // the same private stream immediately.
                    let retry = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                148,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let retry_payload = next_text(&mut writes).await;
                    assert_eq!(retry_payload["id"].as_u64(), Some(148));
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    sleep(Duration::from_millis(40)).await;
                    assert!(
                        timeout(Duration::from_millis(20), events.recv())
                            .await
                            .is_err()
                    );
                    ws.on_message_with_session(
                        json!({"result": null, "id": 148}).to_string(),
                        connection,
                        1,
                    )
                    .await;
                    retry.await.unwrap().unwrap();
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Acknowledged
                    );
                });
            }

            #[test]
            fn cancellation_after_ack_delivery_but_before_commit_releases_assignment() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                49,
                                Duration::from_millis(200),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );

                    // Hold the assignment lock so the waiter receives its ACK
                    // but cannot commit ownership before it is cancelled.
                    let assignment_lock = ws.connection_streams.lock().await;
                    ws.on_message_with_session(
                        json!({"result": null, "id": 49}).to_string(),
                        connection.clone(),
                        1,
                    )
                    .await;
                    tokio::task::yield_now().await;
                    subscribe.abort();
                    let _ = subscribe.await;
                    drop(assignment_lock);
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Cancelled
                    );

                    assert!(
                        eventually_async(Duration::from_millis(200), || {
                            let ws = ws.clone();
                            async move { !ws.is_subscribed(LISTEN_KEY).await }
                        })
                        .await
                    );
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .confirmed_stream_keys
                            .is_empty()
                    );

                    connection.session_generation.store(2, Ordering::Release);
                    {
                        let mut state = connection.state.lock().await;
                        state.writer_session_generation = Some(2);
                    }
                    ws.on_open(
                        "wss://fstream.binance.com/private/stream".to_string(),
                        connection,
                    )
                    .await;
                    assert!(writes.try_recv().is_err());
                    assert!(
                        timeout(Duration::from_millis(20), events.recv())
                            .await
                            .is_err()
                    );
                });
            }

            #[test]
            fn dispatch_failure_owner_survives_waiter_abort_and_cleans_before_terminal() {
                // This case intentionally blocks inside the synchronous
                // observer. A dedicated two-worker runtime keeps the test
                // driver runnable; the crate's shared current-thread runtime
                // would make the barrier depend on an unrelated parallel test.
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("dispatch-failure test runtime");
                runtime.block_on(async {
                    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let dispatch_barrier = Arc::new(std::sync::Barrier::new(2));
                    let (dispatch_entered_tx, dispatch_entered_rx) = std::sync::mpsc::channel();
                    let (terminal_tx, terminal_rx) = std::sync::mpsc::channel();
                    let observer = {
                        let outcomes = outcomes.clone();
                        let dispatch_barrier = dispatch_barrier.clone();
                        StreamSubscriptionObserver::new(move |event| {
                            outcomes.lock().unwrap().push(event.outcome.clone());
                            match event.outcome {
                                StreamSubscriptionOutcome::Dispatched => {
                                    let _ = dispatch_entered_tx.send(());
                                    dispatch_barrier.wait();
                                }
                                StreamSubscriptionOutcome::DispatchFailed => {
                                    let _ = terminal_tx.send(());
                                }
                                _ => {}
                            }
                        })
                    };
                    let (ws, connection, writes) =
                        setup_private_connection_with_observers(1, None, Some(observer)).await;
                    drop(writes);

                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                50,
                                Duration::from_secs(30),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    dispatch_entered_rx
                        .recv_timeout(Duration::from_millis(200))
                        .expect("synchronous Dispatched observer was not entered");

                    // The terminal owner must block here after atomically
                    // removing pending state but before publishing the event.
                    let assignment_lock = ws.connection_streams.lock().await;
                    dispatch_barrier.wait();
                    assert!(
                        eventually_async(Duration::from_millis(200), || {
                            let connection = connection.clone();
                            async move {
                                !connection
                                    .state
                                    .lock()
                                    .await
                                    .pending_stream_subscriptions
                                    .contains_key(&50)
                            }
                        })
                        .await
                    );
                    subscribe.abort();
                    assert!(subscribe.await.unwrap_err().is_cancelled());
                    assert!(matches!(
                        terminal_rx.try_recv(),
                        Err(std::sync::mpsc::TryRecvError::Empty)
                    ));

                    drop(assignment_lock);
                    terminal_rx
                        .recv_timeout(Duration::from_millis(200))
                        .expect("detached terminal owner did not publish DispatchFailed");
                    assert_eq!(
                        *outcomes.lock().unwrap(),
                        vec![
                            StreamSubscriptionOutcome::Dispatched,
                            StreamSubscriptionOutcome::DispatchFailed,
                        ]
                    );
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .unconfirmed_stream_reservations
                            .is_empty()
                    );
                });
            }

            #[test]
            fn arbitrary_internal_scope_is_collapsed_in_subscription_diagnostics() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, mut writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    {
                        let mut state = connection.state.lock().await;
                        state.url_path = Some(LISTEN_KEY.to_string());
                    }
                    let subscribe = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                52,
                                Duration::from_millis(100),
                                Some(LISTEN_KEY),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    let dispatched = next_event(&mut events).await;
                    assert_eq!(
                        dispatched.context.path_scope,
                        StreamSubscriptionScope::Other
                    );
                    assert!(!format!("{dispatched:?}").contains(LISTEN_KEY));

                    ws.on_message_with_session(
                        json!({"code": 2, "msg": LISTEN_KEY, "id": 52}).to_string(),
                        connection,
                        1,
                    )
                    .await;
                    let error = subscribe.await.unwrap().unwrap_err();
                    let StreamSubscriptionError::Rejected { context, code: 2 } = &error else {
                        panic!("expected correlated rejection, got {error:?}");
                    };
                    assert_eq!(context.path_scope, StreamSubscriptionScope::Other);
                    assert!(!format!("{error:?}").contains(LISTEN_KEY));
                    assert!(!format!("{error}").contains(LISTEN_KEY));
                    let rejected = next_event(&mut events).await;
                    assert_eq!(rejected.context.path_scope, StreamSubscriptionScope::Other);
                    assert!(!format!("{rejected:?}").contains(LISTEN_KEY));
                });
            }

            #[test]
            fn disconnected_observer_retry_cannot_dispatch_on_closing_session() {
                TOKIO_SHARED_RT.block_on(async {
                    let outcomes = Arc::new(std::sync::Mutex::new(Vec::new()));
                    let (retry_tx, mut retry_rx) = unbounded_channel();
                    let observer = {
                        let outcomes = outcomes.clone();
                        StreamSubscriptionObserver::new(move |event| {
                            outcomes.lock().unwrap().push(event.outcome.clone());
                            if event.outcome == StreamSubscriptionOutcome::Disconnected {
                                let _ = retry_tx.send(());
                            }
                        })
                    };
                    let (ws, connection, mut writes) =
                        setup_private_connection_with_observers(1, None, Some(observer)).await;
                    let initial = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            ws.subscribe_one_confirmed(
                                LISTEN_KEY.to_string(),
                                61,
                                Duration::from_secs(30),
                                Some("private"),
                            )
                            .await
                        })
                    };
                    let initial_payload = next_text(&mut writes).await;
                    assert_eq!(initial_payload["id"].as_u64(), Some(61));

                    let retry = {
                        let ws = ws.clone();
                        tokio::spawn(async move {
                            retry_rx
                                .recv()
                                .await
                                .expect("Disconnected observer did not signal retry");
                            ws.subscribe_one_confirmed(
                                "replacement-private-stream".to_string(),
                                62,
                                Duration::from_millis(20),
                                Some("private"),
                            )
                            .await
                        })
                    };

                    ws.disconnect().await.unwrap();
                    assert!(matches!(
                        initial.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::Disconnected { .. }
                    ));
                    assert!(matches!(
                        retry.await.unwrap().unwrap_err(),
                        StreamSubscriptionError::NoConnection { request_id: 62 }
                    ));
                    assert_eq!(
                        *outcomes.lock().unwrap(),
                        vec![
                            StreamSubscriptionOutcome::Dispatched,
                            StreamSubscriptionOutcome::Disconnected,
                        ]
                    );
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .pending_stream_subscriptions
                            .is_empty()
                    );
                    while let Ok(message) = writes.try_recv() {
                        assert!(
                            !matches!(message, Message::Text(_)),
                            "retry dispatched a payload onto the closing session"
                        );
                    }
                });
            }

            #[test]
            fn disconnected_writer_fails_without_leaking_or_retaining_stream() {
                TOKIO_SHARED_RT.block_on(async {
                    let (ws, connection, _writes) = setup_private_connection(1).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    {
                        let mut state = connection.state.lock().await;
                        state.ws_write_tx = None;
                        state.writer_session_generation = None;
                    }
                    let error = ws
                        .clone()
                        .subscribe_one_confirmed(
                            LISTEN_KEY.to_string(),
                            51,
                            Duration::from_millis(10),
                            Some("private"),
                        )
                        .await
                        .unwrap_err();
                    assert!(matches!(
                        error,
                        StreamSubscriptionError::NotConnected { .. }
                    ));
                    assert!(!format!("{error:?}").contains(LISTEN_KEY));
                    assert!(!ws.is_subscribed(LISTEN_KEY).await);
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::DispatchFailed
                    );
                    assert!(
                        connection
                            .state
                            .lock()
                            .await
                            .pending_stream_subscriptions
                            .is_empty()
                    );
                });
            }

            #[cfg(feature = "derivatives_trading_usds_futures")]
            mod routed_control {
                use super::*;

                async fn setup_routed_connection(
                    scope: RoutedStreamScope,
                    generation: u64,
                ) -> (
                    Arc<WebsocketStreams>,
                    Arc<WebsocketConnection>,
                    tokio::sync::mpsc::UnboundedReceiver<Message>,
                ) {
                    let config = ConfigurationWebsocketStreams {
                        ws_url: Some("wss://fstream.binance.com".to_string()),
                        mode: WebsocketMode::Single,
                        reconnect_delay: 500,
                        time_unit: None,
                        raw_frame_observer: None,
                        stream_subscription_observer: None,
                        agent: None,
                        user_agent: build_user_agent("confirmed-routed-test"),
                    };
                    let ws = WebsocketStreams::new(
                        config,
                        vec![WebsocketConnection::new("routed-c1")],
                        vec![],
                    );
                    let connection = ws.common.connection_pool[0].clone();
                    let (writer, receiver) = unbounded_channel();
                    connection
                        .session_generation
                        .store(generation, Ordering::Release);
                    {
                        let mut state = connection.state.lock().await;
                        state.url_path = Some(scope.as_path_scope().to_string());
                        state.ws_write_tx = Some(writer);
                        state.writer_session_generation = Some(generation);
                        state.reconnection_pending = false;
                        state.close_initiated = false;
                    }
                    (ws, connection, receiver)
                }

                fn target(
                    connection: &WebsocketConnection,
                    generation: u64,
                    scope: RoutedStreamScope,
                ) -> RoutedStreamTarget {
                    RoutedStreamTarget::new(connection.id.clone(), generation, scope)
                }

                async fn assert_cancelled_routed_response(
                    response: Value,
                    request_id: u32,
                    response_queued_before_guard_cleanup: bool,
                ) {
                    let (ws, connection, mut writes) =
                        setup_routed_connection(RoutedStreamScope::Public, 13).await;
                    let (_subscription, mut events) = observe_subscription_events(&ws);
                    let request = {
                        let ws = ws.clone();
                        let target = target(&connection, 13, RoutedStreamScope::Public);
                        tokio::spawn(async move {
                            ws.subscribe_routed_confirmed(
                                target,
                                vec!["btcusdt@depth".to_string()],
                                request_id,
                                Duration::from_millis(100),
                            )
                            .await
                        })
                    };
                    let _ = next_text(&mut writes).await;
                    assert_eq!(
                        next_event(&mut events).await.outcome,
                        StreamSubscriptionOutcome::Dispatched
                    );

                    if response_queued_before_guard_cleanup {
                        // Queue the wire response on the state mutex first,
                        // then drop the receiver while both terminal owners are
                        // unable to clean the pending entry. Tokio's FIFO mutex
                        // ordering makes the response path win deterministically.
                        let state = connection.state.lock().await;
                        let mut response_delivery = Box::pin(ws.on_message_with_session(
                            response.to_string(),
                            connection.clone(),
                            13,
                        ));
                        assert!(futures::poll!(response_delivery.as_mut()).is_pending());
                        request.abort();
                        assert!(request.await.unwrap_err().is_cancelled());
                        drop(state);
                        response_delivery.await;
                    } else {
                        request.abort();
                        assert!(request.await.unwrap_err().is_cancelled());
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Cancelled
                        );
                        ws.on_message_with_session(response.to_string(), connection.clone(), 13)
                            .await;
                    }

                    if response_queued_before_guard_cleanup {
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Cancelled
                        );
                    }
                    assert!(
                        timeout(Duration::from_millis(20), events.recv())
                            .await
                            .is_err(),
                        "cancelled routed control emitted more than one terminal outcome"
                    );
                    let state = connection.state.lock().await;
                    assert!(!state.pending_stream_subscriptions.contains_key(&request_id));
                    assert_eq!(
                        state.stream_request_id_generations.get(&request_id),
                        Some(&13)
                    );
                }

                #[test]
                fn dropped_receiver_beats_ack_when_wire_response_owns_state_first() {
                    TOKIO_SHARED_RT.block_on(assert_cancelled_routed_response(
                        json!({"result": null, "id": 130}),
                        130,
                        true,
                    ));
                }

                #[test]
                fn dropped_receiver_beats_reject_when_wire_response_owns_state_first() {
                    TOKIO_SHARED_RT.block_on(assert_cancelled_routed_response(
                        json!({"code": 2, "msg": "invalid request", "id": 131}),
                        131,
                        true,
                    ));
                }

                #[test]
                fn late_ack_is_ignored_after_cancellation_guard_cleanup() {
                    TOKIO_SHARED_RT.block_on(assert_cancelled_routed_response(
                        json!({"result": null, "id": 132}),
                        132,
                        false,
                    ));
                }

                #[test]
                fn late_reject_is_ignored_after_cancellation_guard_cleanup() {
                    TOKIO_SHARED_RT.block_on(assert_cancelled_routed_response(
                        json!({"code": 2, "msg": "invalid request", "id": 133}),
                        133,
                        false,
                    ));
                }

                #[test]
                fn subscribe_and_unsubscribe_are_sorted_numeric_confirmed_and_ownerless() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 7).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let subscribe = {
                            let ws = ws.clone();
                            let target = target(&connection, 7, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec![
                                        "ethusdt@depth".to_string(),
                                        "btcusdt@depth".to_string(),
                                        "ethusdt@depth".to_string(),
                                    ],
                                    700,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };

                        let payload = next_text(&mut writes).await;
                        assert_eq!(payload["method"], "SUBSCRIBE");
                        assert_eq!(payload["params"], json!(["btcusdt@depth", "ethusdt@depth"]));
                        assert_eq!(payload["id"].as_u64(), Some(700));
                        assert!(payload["id"].is_number());
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );

                        ws.on_message_with_session(
                            json!({"result": null, "id": 700}).to_string(),
                            connection.clone(),
                            7,
                        )
                        .await;
                        let ack = subscribe.await.unwrap().unwrap();
                        assert_eq!(ack.context.connection_id, connection.id);
                        assert_eq!(ack.context.session_generation, 7);
                        assert_eq!(ack.context.request_id, 700);
                        assert_eq!(ack.context.path_scope, StreamSubscriptionScope::Public);
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Acknowledged
                        );

                        {
                            let state = connection.state.lock().await;
                            assert!(state.stream_callbacks.is_empty());
                            assert!(state.confirmed_stream_keys.is_empty());
                            assert!(state.pending_subscriptions.is_empty());
                        }
                        assert!(ws.connection_streams.lock().await.is_empty());
                        assert_eq!(
                            ws.get_reconnect_url(String::new(), connection.clone())
                                .await,
                            "wss://fstream.binance.com/public/stream"
                        );

                        let unsubscribe = {
                            let ws = ws.clone();
                            let target = target(&connection, 7, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.unsubscribe_routed_confirmed(
                                    target,
                                    vec!["ethusdt@depth".to_string(), "btcusdt@depth".to_string()],
                                    701,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };
                        let payload = next_text(&mut writes).await;
                        assert_eq!(payload["method"], "UNSUBSCRIBE");
                        assert_eq!(payload["params"], json!(["btcusdt@depth", "ethusdt@depth"]));
                        assert_eq!(payload["id"].as_u64(), Some(701));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        ws.on_message_with_session(
                            json!({"result": null, "id": 701}).to_string(),
                            connection.clone(),
                            7,
                        )
                        .await;
                        unsubscribe.await.unwrap().unwrap();
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Acknowledged
                        );

                        connection.session_generation.store(8, Ordering::Release);
                        {
                            let mut state = connection.state.lock().await;
                            state.writer_session_generation = Some(8);
                        }
                        ws.on_open(
                            "wss://fstream.binance.com/public/stream".to_string(),
                            connection,
                        )
                        .await;
                        assert!(
                            writes.try_recv().is_err(),
                            "SDK replayed an application-owned routed subscription"
                        );
                    });
                }

                #[test]
                fn duplicate_request_id_is_a_terminal_protocol_error() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 10).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let first = {
                            let ws = ws.clone();
                            let target = target(&connection, 10, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    100,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        ws.on_message_with_session(
                            json!({"result": null, "id": 100}).to_string(),
                            connection.clone(),
                            10,
                        )
                        .await;
                        first.await.unwrap().unwrap();
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Acknowledged
                        );

                        let duplicate = ws
                            .subscribe_routed_confirmed(
                                target(&connection, 10, RoutedStreamScope::Public),
                                vec!["ethusdt@depth".to_string()],
                                100,
                                Duration::from_millis(200),
                            )
                            .await;
                        assert!(matches!(
                            duplicate,
                            Err(StreamSubscriptionError::DuplicateRequestId { .. })
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::ProtocolError
                        );
                        assert!(writes.try_recv().is_err());
                    });
                }

                #[test]
                fn ack_requires_exact_connection_generation_scope_and_request() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Market, 5).await;
                        let request = {
                            let ws = ws.clone();
                            let target = target(&connection, 5, RoutedStreamScope::Market);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@aggTrade".to_string()],
                                    u32::MAX,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };
                        let payload = next_text(&mut writes).await;
                        assert_eq!(payload["id"].as_u64(), Some(u64::from(u32::MAX)));

                        let other_connection = WebsocketConnection::new("other-routed-c1");
                        ws.on_message_with_session(
                            json!({"result": null, "id": u32::MAX}).to_string(),
                            other_connection,
                            5,
                        )
                        .await;
                        ws.on_message_with_session(
                            json!({"result": null, "id": u32::MAX}).to_string(),
                            connection.clone(),
                            4,
                        )
                        .await;
                        ws.on_message_with_session(
                            json!({"result": null, "id": u32::MAX - 1}).to_string(),
                            connection.clone(),
                            5,
                        )
                        .await;
                        tokio::task::yield_now().await;
                        assert!(!request.is_finished());

                        ws.on_message_with_session(
                            json!({"result": null, "id": u32::MAX}).to_string(),
                            connection,
                            5,
                        )
                        .await;
                        let ack = request.await.unwrap().unwrap();
                        assert_eq!(ack.context.request_id, u32::MAX);
                        assert_eq!(ack.context.path_scope, StreamSubscriptionScope::Market);
                    });
                }

                #[test]
                fn stale_scope_and_invalid_topics_fail_before_dispatch() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 2).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);

                        let stale = ws
                            .subscribe_routed_confirmed(
                                target(&connection, 2, RoutedStreamScope::Market),
                                vec!["btcusdt@depth".to_string()],
                                20,
                                Duration::from_millis(20),
                            )
                            .await
                            .unwrap_err();
                        assert!(matches!(
                            stale,
                            StreamSubscriptionError::SessionReplaced { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::SessionReplaced
                        );

                        let invalid = ws
                            .subscribe_routed_confirmed(
                                target(&connection, 2, RoutedStreamScope::Public),
                                vec![String::new()],
                                21,
                                Duration::from_millis(20),
                            )
                            .await
                            .unwrap_err();
                        assert!(matches!(invalid, StreamSubscriptionError::Protocol { .. }));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::ProtocolError
                        );
                        assert!(writes.try_recv().is_err());
                    });
                }

                #[test]
                fn correlated_reject_and_protocol_error_are_terminal() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 3).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);

                        let rejected = {
                            let ws = ws.clone();
                            let target = target(&connection, 3, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    30,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        ws.on_message_with_session(
                            json!({"code": 2, "msg": "invalid request", "id": 30}).to_string(),
                            connection.clone(),
                            3,
                        )
                        .await;
                        assert!(matches!(
                            rejected.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::Rejected { code: 2, .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Rejected { code: 2 }
                        );

                        let protocol = {
                            let ws = ws.clone();
                            let target = target(&connection, 3, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.unsubscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    31,
                                    Duration::from_millis(200),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        ws.on_message_with_session(
                            json!({"result": true, "id": 31}).to_string(),
                            connection,
                            3,
                        )
                        .await;
                        assert!(matches!(
                            protocol.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::Protocol { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::ProtocolError
                        );
                    });
                }

                #[test]
                fn timeout_and_dispatch_failure_are_terminal() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Market, 4).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let timed_out = {
                            let ws = ws.clone();
                            let target = target(&connection, 4, RoutedStreamScope::Market);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@markPrice".to_string()],
                                    40,
                                    Duration::from_millis(20),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        assert!(matches!(
                            timed_out.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::Timeout { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::TimedOut
                        );

                        drop(writes);
                        let failed = ws
                            .subscribe_routed_confirmed(
                                target(&connection, 4, RoutedStreamScope::Market),
                                vec!["ethusdt@markPrice".to_string()],
                                41,
                                Duration::from_millis(20),
                            )
                            .await
                            .unwrap_err();
                        assert!(matches!(
                            failed,
                            StreamSubscriptionError::NotConnected { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::DispatchFailed
                        );
                    });
                }

                #[test]
                fn pending_request_observes_session_replacement() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 6).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let request = {
                            let ws = ws.clone();
                            let target = target(&connection, 6, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    60,
                                    Duration::from_secs(30),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );

                        connection.session_generation.store(7, Ordering::Release);
                        {
                            let mut state = connection.state.lock().await;
                            state.writer_session_generation = Some(7);
                        }
                        ws.on_open(
                            "wss://fstream.binance.com/public/stream".to_string(),
                            connection,
                        )
                        .await;
                        assert!(matches!(
                            request.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::SessionReplaced { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::SessionReplaced
                        );
                    });
                }

                #[test]
                fn new_generation_reuses_caller_id_before_open_cleanup() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 11).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let first = {
                            let ws = ws.clone();
                            let target = target(&connection, 11, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    110,
                                    Duration::from_secs(30),
                                )
                                .await
                            })
                        };
                        let first_payload = next_text(&mut writes).await;
                        assert_eq!(first_payload["id"].as_u64(), Some(110));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );

                        // The transport publishes Open for generation 12 before
                        // WebsocketStreams::on_open gets to drain generation 11.
                        // A synchronous Open observer may immediately reissue its
                        // caller-owned desired set with the same numeric id.
                        connection.session_generation.store(12, Ordering::Release);
                        {
                            let mut state = connection.state.lock().await;
                            state.writer_session_generation = Some(12);
                        }
                        let second = {
                            let ws = ws.clone();
                            let target = target(&connection, 12, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["ethusdt@depth".to_string()],
                                    110,
                                    Duration::from_secs(30),
                                )
                                .await
                            })
                        };
                        let second_payload = next_text(&mut writes).await;
                        assert_eq!(second_payload["id"].as_u64(), Some(110));
                        assert!(matches!(
                            first.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::SessionReplaced { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::SessionReplaced
                        );
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );

                        // A response still arriving from generation 11 cannot
                        // complete the generation-12 owner of the same wire id.
                        ws.on_message_with_session(
                            json!({"result": null, "id": 110}).to_string(),
                            connection.clone(),
                            11,
                        )
                        .await;
                        tokio::task::yield_now().await;
                        assert!(!second.is_finished());

                        ws.on_message_with_session(
                            json!({"result": null, "id": 110}).to_string(),
                            connection,
                            12,
                        )
                        .await;
                        let ack = second.await.unwrap().unwrap();
                        assert_eq!(ack.context.session_generation, 12);
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Acknowledged
                        );
                    });
                }

                #[test]
                fn pending_request_observes_disconnect() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Market, 8).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let request = {
                            let ws = ws.clone();
                            let target = target(&connection, 8, RoutedStreamScope::Market);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@aggTrade".to_string()],
                                    80,
                                    Duration::from_secs(30),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        ws.disconnect().await.unwrap();
                        assert!(matches!(
                            request.await.unwrap().unwrap_err(),
                            StreamSubscriptionError::Disconnected { .. }
                        ));
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Disconnected
                        );
                    });
                }

                #[test]
                fn cancelled_waiter_removes_only_exact_pending_control() {
                    TOKIO_SHARED_RT.block_on(async {
                        let (ws, connection, mut writes) =
                            setup_routed_connection(RoutedStreamScope::Public, 9).await;
                        let (_subscription, mut events) = observe_subscription_events(&ws);
                        let request = {
                            let ws = ws.clone();
                            let target = target(&connection, 9, RoutedStreamScope::Public);
                            tokio::spawn(async move {
                                ws.subscribe_routed_confirmed(
                                    target,
                                    vec!["btcusdt@depth".to_string()],
                                    90,
                                    Duration::from_secs(30),
                                )
                                .await
                            })
                        };
                        let _ = next_text(&mut writes).await;
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Dispatched
                        );
                        request.abort();
                        assert!(request.await.unwrap_err().is_cancelled());
                        assert_eq!(
                            next_event(&mut events).await.outcome,
                            StreamSubscriptionOutcome::Cancelled
                        );
                        let state = connection.state.lock().await;
                        assert!(!state.pending_stream_subscriptions.contains_key(&90));
                        assert_eq!(state.stream_request_id_generations.get(&90), Some(&9));
                        assert!(state.confirmed_stream_keys.is_empty());
                        assert!(state.stream_callbacks.is_empty());
                    });
                }
            }
        }

        mod on_open {
            use super::*;

            #[test]
            fn sends_pending_subscriptions() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                        st.pending_subscriptions.push_back("foo".to_string());
                        st.pending_subscriptions.push_back("bar".to_string());
                    }
                    ws.on_open("ws://example.com".to_string(), conn.clone())
                        .await;
                    let msg = rx.recv().await.expect("no subscription sent");
                    if let Message::Text(txt) = msg {
                        let v: Value = serde_json::from_str(&txt).unwrap();
                        assert_eq!(v["method"], "SUBSCRIBE");
                        let params = v["params"].as_array().unwrap();
                        assert_eq!(
                            params,
                            &vec![Value::String("foo".into()), Value::String("bar".into())]
                        );
                    } else {
                        panic!("unexpected message: {msg:?}");
                    }
                    let st_after = conn.state.lock().await;
                    assert!(st_after.pending_subscriptions.is_empty());
                });
            }

            #[test]
            fn with_no_pending_subscriptions_sends_nothing() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let (tx, mut rx) = unbounded_channel();
                    {
                        let mut st = conn.state.lock().await;
                        st.ws_write_tx = Some(tx);
                    }
                    ws.on_open("ws://example.com".to_string(), conn.clone())
                        .await;
                    assert!(rx.try_recv().is_err(), "unexpected message sent");
                });
            }

            #[test]
            fn clears_pending_without_write_channel() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    {
                        let mut st = conn.state.lock().await;
                        st.pending_subscriptions.push_back("solo".to_string());
                    }
                    ws.on_open("ws://example.com".to_string(), conn.clone())
                        .await;
                    let st_after = conn.state.lock().await;
                    assert!(st_after.pending_subscriptions.is_empty());
                });
            }
        }

        mod on_message {
            use super::*;

            #[test]
            fn invokes_registered_callback() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let called = Arc::new(AtomicBool::new(false));
                    let called_clone = called.clone();

                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks
                            .entry("stream1".to_string())
                            .or_default()
                            .push(
                                (Box::new(move |_: &Value| {
                                    called_clone.store(true, Ordering::SeqCst);
                                })
                                    as Box<dyn Fn(&Value) + Send + Sync>)
                                    .into(),
                            );
                    }

                    let msg = json!({
                        "stream": "stream1",
                        "data": { "key": "value" }
                    })
                    .to_string();

                    ws.on_message(msg, conn.clone()).await;

                    assert!(called.load(Ordering::SeqCst));
                });
            }

            #[test]
            fn invokes_all_registered_callbacks() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let counter = Arc::new(AtomicUsize::new(0));

                    {
                        let mut st = conn.state.lock().await;
                        let entry = st.stream_callbacks.entry("s".into()).or_default();
                        let c1 = counter.clone();
                        entry.push(
                            (Box::new(move |_: &Value| {
                                c1.fetch_add(1, Ordering::SeqCst);
                            }) as Box<dyn Fn(&Value) + Send + Sync>)
                                .into(),
                        );
                        let c2 = counter.clone();
                        entry.push(
                            (Box::new(move |_: &Value| {
                                c2.fetch_add(1, Ordering::SeqCst);
                            }) as Box<dyn Fn(&Value) + Send + Sync>)
                                .into(),
                        );
                    }

                    let msg = json!({"stream":"s","data":42}).to_string();
                    ws.on_message(msg, conn.clone()).await;

                    assert_eq!(counter.load(Ordering::SeqCst), 2);
                });
            }

            #[test]
            fn handles_null_data_field() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let called = Arc::new(AtomicUsize::new(0));
                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks.entry("n".into()).or_default().push(
                            (Box::new({
                                let c = called.clone();
                                move |data: &Value| {
                                    if data.is_null() {
                                        c.fetch_add(1, Ordering::SeqCst);
                                    }
                                }
                            }) as Box<dyn Fn(&Value) + Send + Sync>)
                                .into(),
                        );
                    }
                    let msg = json!({"stream":"n","data":null}).to_string();
                    ws.on_message(msg, conn.clone()).await;
                    assert_eq!(called.load(Ordering::SeqCst), 1);
                });
            }

            #[test]
            fn with_invalid_json_does_not_panic() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let bad = "not a json";
                    ws.on_message(bad.to_string(), conn.clone()).await;
                });
            }

            #[test]
            fn without_stream_field_does_nothing() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let msg = json!({ "data": { "foo": 1 } }).to_string();
                    ws.on_message(msg, conn.clone()).await;
                });
            }

            #[test]
            fn with_unregistered_stream_does_not_panic() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];
                    let msg = json!({
                        "stream": "nope",
                        "data": { "foo": 1 }
                    })
                    .to_string();
                    ws.on_message(msg, conn.clone()).await;
                });
            }

            #[test]
            fn invokes_registered_callback_with_url_path_key() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];

                    {
                        let mut st = conn.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    let called = Arc::new(AtomicBool::new(false));
                    let called_clone = called.clone();

                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks
                            .entry("path1::stream1".to_string())
                            .or_default()
                            .push(
                                (Box::new(move |_: &Value| {
                                    called_clone.store(true, Ordering::SeqCst);
                                })
                                    as Box<dyn Fn(&Value) + Send + Sync>)
                                    .into(),
                            );
                    }

                    let msg = json!({
                        "stream": "stream1",
                        "data": { "key": "value" }
                    })
                    .to_string();

                    ws.on_message(msg, conn.clone()).await;

                    assert!(called.load(Ordering::SeqCst));
                });
            }

            #[test]
            fn does_not_invoke_callback_when_url_path_mismatch() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];

                    {
                        let mut st = conn.state.lock().await;
                        st.url_path = Some("path2".to_string());
                    }

                    let called = Arc::new(AtomicBool::new(false));
                    let called_clone = called.clone();

                    {
                        let mut st = conn.state.lock().await;
                        st.stream_callbacks
                            .entry("path1::stream1".to_string())
                            .or_default()
                            .push(
                                (Box::new(move |_: &Value| {
                                    called_clone.store(true, Ordering::SeqCst);
                                })
                                    as Box<dyn Fn(&Value) + Send + Sync>)
                                    .into(),
                            );
                    }

                    let msg = json!({
                        "stream": "stream1",
                        "data": { "key": "value" }
                    })
                    .to_string();

                    ws.on_message(msg, conn.clone()).await;

                    assert!(!called.load(Ordering::SeqCst));
                });
            }

            #[test]
            fn invokes_only_callbacks_for_current_url_path_when_both_exist() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let conn = &ws.common.connection_pool[0];

                    {
                        let mut st = conn.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    let c1 = Arc::new(AtomicUsize::new(0));
                    let c2 = Arc::new(AtomicUsize::new(0));

                    {
                        let mut st = conn.state.lock().await;

                        let a = c1.clone();
                        st.stream_callbacks
                            .entry("path1::s".to_string())
                            .or_default()
                            .push(
                                (Box::new(move |_: &Value| {
                                    a.fetch_add(1, Ordering::SeqCst);
                                })
                                    as Box<dyn Fn(&Value) + Send + Sync>)
                                    .into(),
                            );

                        let b = c2.clone();
                        st.stream_callbacks
                            .entry("path2::s".to_string())
                            .or_default()
                            .push(
                                (Box::new(move |_: &Value| {
                                    b.fetch_add(1, Ordering::SeqCst);
                                })
                                    as Box<dyn Fn(&Value) + Send + Sync>)
                                    .into(),
                            );
                    }

                    let msg = json!({"stream":"s","data":42}).to_string();
                    ws.on_message(msg, conn.clone()).await;

                    assert_eq!(c1.load(Ordering::SeqCst), 1);
                    assert_eq!(c2.load(Ordering::SeqCst), 0);
                });
            }
        }

        mod get_reconnect_url {
            use super::*;

            #[test]
            fn single_stream_reconnect_url() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let c0 = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("s1".to_string(), c0.clone());
                    }
                    let url = ws.get_reconnect_url("default_url".into(), c0).await;
                    assert_eq!(url, "ws://example.com/stream?streams=s1");
                });
            }

            #[test]
            fn multiple_streams_same_connection() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let c0 = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("a".to_string(), c0.clone());
                        map.insert("b".to_string(), c0.clone());
                    }
                    let url = ws.get_reconnect_url("default_url".into(), c0).await;
                    let suffix = url
                        .strip_prefix("ws://example.com/stream?streams=")
                        .unwrap();
                    let parts: Vec<_> = suffix.split('&').next().unwrap().split('/').collect();
                    let set = parts.into_iter().collect::<std::collections::HashSet<_>>();
                    assert_eq!(set, ["a", "b"].iter().copied().collect());
                });
            }

            #[test]
            fn reconnect_url_with_time_unit() {
                TOKIO_SHARED_RT.block_on(async {
                    let mut ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    Arc::get_mut(&mut ws).unwrap().configuration.time_unit =
                        Some(TimeUnit::Microsecond);
                    let c0 = ws.common.connection_pool[0].clone();
                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("x".to_string(), c0.clone());
                    }
                    let url = ws.get_reconnect_url("default_url".into(), c0).await;
                    assert_eq!(
                        url,
                        "ws://example.com/stream?streams=x&timeUnit=microsecond"
                    );
                });
            }

            #[test]
            fn reconnect_url_uses_url_path_from_connection_state() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let c0 = ws.common.connection_pool[0].clone();

                    {
                        let mut st = c0.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::s1".to_string(), c0.clone());
                    }

                    let url = ws.get_reconnect_url("default_url".into(), c0).await;
                    assert_eq!(url, "ws://example.com/path1/stream?streams=s1");
                });
            }

            #[test]
            fn reconnect_url_strips_prefix_from_multiple_keys_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let c0 = ws.common.connection_pool[0].clone();

                    {
                        let mut st = c0.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::a".to_string(), c0.clone());
                        map.insert("path1::b".to_string(), c0.clone());
                    }

                    let url = ws.get_reconnect_url("default_url".into(), c0).await;

                    let suffix = url
                        .strip_prefix("ws://example.com/path1/stream?streams=")
                        .unwrap();
                    let parts: Vec<_> = suffix.split('&').next().unwrap().split('/').collect();
                    let set = parts.into_iter().collect::<std::collections::HashSet<_>>();
                    assert_eq!(set, ["a", "b"].iter().copied().collect());
                });
            }

            #[test]
            fn reconnect_url_with_url_path_and_time_unit() {
                TOKIO_SHARED_RT.block_on(async {
                    let mut ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    Arc::get_mut(&mut ws).unwrap().configuration.time_unit =
                        Some(TimeUnit::Microsecond);

                    let c0 = ws.common.connection_pool[0].clone();

                    {
                        let mut st = c0.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::x".to_string(), c0.clone());
                    }

                    let url = ws.get_reconnect_url("default_url".into(), c0).await;
                    assert_eq!(
                        url,
                        "ws://example.com/path1/stream?streams=x&timeUnit=microsecond"
                    );
                });
            }

            #[test]
            fn reconnect_url_ignores_streams_from_other_connections_even_if_same_path_prefix() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws: Arc<WebsocketStreams> =
                        create_websocket_streams(Some("ws://example.com"), None, None);
                    let c0 = ws.common.connection_pool[0].clone();
                    let c1 = ws.common.connection_pool[1].clone();

                    {
                        let mut st = c0.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }
                    {
                        let mut st = c1.state.lock().await;
                        st.url_path = Some("path1".to_string());
                    }

                    {
                        let mut map = ws.connection_streams.lock().await;
                        map.insert("path1::a".to_string(), c0.clone());
                        map.insert("path1::b".to_string(), c1.clone());
                    }

                    let url = ws.get_reconnect_url("default_url".into(), c0).await;

                    let suffix = url
                        .strip_prefix("ws://example.com/path1/stream?streams=")
                        .unwrap();
                    let parts: Vec<_> = suffix.split('&').next().unwrap().split('/').collect();
                    let set = parts.into_iter().collect::<std::collections::HashSet<_>>();
                    assert_eq!(set, ["a"].iter().copied().collect());
                });
            }
        }
    }

    mod websocket_stream {
        use super::*;

        mod on {
            use super::*;

            #[test]
            fn registers_callback_and_stream_callback_for_websocket_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s1".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();

                    let key = ws_base.stream_key(&stream_name, None);

                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(key.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert(key.clone(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        callback: Mutex::new(None),
                        url_path: None,
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("message", |_| {}).await;

                    let cb_guard = stream.callback.lock().await;
                    assert!(cb_guard.is_some());

                    let cbs = {
                        let state = conn.state.lock().await;
                        state.stream_callbacks.get(&key).unwrap().clone()
                    };
                    assert_eq!(cbs.len(), 1);
                });
            }

            #[test]
            fn message_twice_registers_two_wrappers_for_websocket_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s2".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();

                    let key = ws_base.stream_key(&stream_name, None);

                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(key.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert(key.clone(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("message", |_| {}).await;
                    stream.on("message", |_| {}).await;

                    let state = conn.state.lock().await;
                    let callbacks = state.stream_callbacks.get(&key).unwrap();
                    assert_eq!(callbacks.len(), 2);
                });
            }

            #[test]
            fn ignores_non_message_event_for_websocket_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: "s".into(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });
                    stream.on("open", |_| {}).await;
                    let guard = stream.callback.lock().await;
                    assert!(guard.is_none());
                });
            }

            #[test]
            fn registers_callback_and_stream_callback_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);

                    {
                        let mut stream_callbacks = ws_base.stream_callbacks.lock().await;
                        stream_callbacks.insert("id1".to_string(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: "id1".to_string(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    let called = Arc::new(Mutex::new(false));
                    let called_clone = called.clone();
                    stream
                        .on("message", move |v: Value| {
                            let mut lock = called_clone.blocking_lock();
                            *lock = v == Value::String("x".into());
                        })
                        .await;

                    let cb_guard = stream.callback.lock().await;
                    assert!(cb_guard.is_some());

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    let callbacks = stream_callbacks.get("id1").unwrap();
                    assert_eq!(callbacks.len(), 1);
                });
            }

            #[test]
            fn message_twice_registers_two_wrappers_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: "id2".to_string(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("message", |_| {}).await;
                    stream.on("message", |_| {}).await;

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    let callbacks = stream_callbacks.get("id2").unwrap();
                    assert_eq!(callbacks.len(), 2);
                });
            }

            #[test]
            fn ignores_non_message_event_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: "id3".into(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("open", |_| {}).await;

                    let guard = stream.callback.lock().await;
                    assert!(guard.is_none());

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    assert!(stream_callbacks.get("id3").is_none());
                    assert!(stream_callbacks.is_empty());
                });
            }

            #[test]
            fn registers_callback_for_websocket_streams_with_url_path() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s1".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();

                    let key = ws_base.stream_key(&stream_name, Some("path1"));

                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(key.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert(key.clone(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: Some("path1".to_string()),
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("message", |_| {}).await;

                    let cb_guard = stream.callback.lock().await;
                    assert!(cb_guard.is_some());

                    let callbacks = {
                        let state = conn.state.lock().await;
                        state.stream_callbacks.get(&key).unwrap().clone()
                    };
                    assert_eq!(callbacks.len(), 1);
                });
            }

            #[test]
            fn url_path_routes_callback_to_correct_key_when_same_stream_name_used() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s1".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();

                    let key1 = ws_base.stream_key(&stream_name, Some("path1"));
                    let key2 = ws_base.stream_key(&stream_name, Some("path2"));

                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(key1.clone(), conn.clone());
                        map.insert(key2.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state.stream_callbacks.insert(key1.clone(), Vec::new());
                        state.stream_callbacks.insert(key2.clone(), Vec::new());
                    }

                    let stream_path1 = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: Some("path1".to_string()),
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    let stream_path2 = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: Some("path2".to_string()),
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream_path1.on("message", |_| {}).await;
                    stream_path2.on("message", |_| {}).await;

                    let state = conn.state.lock().await;
                    assert_eq!(state.stream_callbacks.get(&key1).unwrap().len(), 1);
                    assert_eq!(state.stream_callbacks.get(&key2).unwrap().len(), 1);
                });
            }
        }

        mod on_message {
            use super::*;

            #[test]
            fn on_message_registers_callback_for_websocket_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();
                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(stream_name.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state
                            .stream_callbacks
                            .insert(stream_name.clone(), Vec::new());
                    }
                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });
                    stream.on_message(|_v| {});
                    let callbacks = &conn.state.lock().await.stream_callbacks[&stream_name];
                    assert_eq!(callbacks.len(), 1);
                });
            }

            #[test]
            fn on_message_twice_registers_two_callbacks_for_websocket_streams() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();
                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(stream_name.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state
                            .stream_callbacks
                            .insert(stream_name.clone(), Vec::new());
                    }
                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });
                    stream.on_message(|_v| {});
                    stream.on_message(|_v| {});
                    let callbacks = &conn.state.lock().await.stream_callbacks[&stream_name];
                    assert_eq!(callbacks.len(), 2);
                });
            }

            #[test]
            fn on_message_registers_callback_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);
                    let identifier = "id1".to_string();

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: identifier.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on_message(|_v: Value| {});

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    let callbacks = stream_callbacks.get(&identifier).unwrap();
                    assert_eq!(callbacks.len(), 1);
                });
            }

            #[test]
            fn on_message_twice_registers_two_callbacks_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);
                    let identifier = "id2".to_string();

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: identifier.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on_message(|_v: Value| {});
                    stream.on_message(|_v: Value| {});

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    let callbacks = stream_callbacks.get(&identifier).unwrap();
                    assert_eq!(callbacks.len(), 2);
                });
            }
        }

        mod unsubscribe {
            use super::*;

            #[test]
            fn without_callback_releases_stream_subscription() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s1".to_string();
                    let url_path = "private";
                    let key = ws_base.stream_key(&stream_name, Some(url_path));
                    let conn = ws_base.common.connection_pool[0].clone();
                    let mut rx = {
                        let (tx, rx) = unbounded_channel::<Message>();
                        let mut state = conn.state.lock().await;
                        state.ws_write_tx = Some(tx);
                        state.url_path = Some(url_path.to_string());
                        rx
                    };
                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(key.clone(), conn.clone());
                    }
                    let mut state = conn.state.lock().await;
                    state.stream_callbacks.insert(key.clone(), vec![]);
                    drop(state);
                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: Some(url_path.to_string()),
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });
                    stream.unsubscribe().await;

                    let message = timeout(Duration::from_millis(200), rx.recv())
                        .await
                        .expect("timed out waiting for UNSUBSCRIBE")
                        .expect("write channel closed before UNSUBSCRIBE");
                    let Message::Text(text) = message else {
                        panic!("expected text UNSUBSCRIBE payload");
                    };
                    let payload: Value = serde_json::from_str(text.as_ref()).unwrap();
                    assert_eq!(payload["method"], "UNSUBSCRIBE");
                    assert_eq!(payload["params"], json!([stream_name]));

                    assert!(!ws_base.connection_streams.lock().await.contains_key(&key));
                    let state = conn.state.lock().await;
                    assert!(!state.stream_callbacks.contains_key(&key));
                });
            }

            #[test]
            fn removes_registered_callback_and_clears_state() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_streams(Some("example.com"), None, None);
                    let stream_name = "s2".to_string();
                    let conn = ws_base.common.connection_pool[0].clone();
                    {
                        let mut map = ws_base.connection_streams.lock().await;
                        map.insert(stream_name.clone(), conn.clone());
                    }
                    {
                        let mut state = conn.state.lock().await;
                        state
                            .stream_callbacks
                            .insert(stream_name.clone(), Vec::new());
                    }
                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketStreams(ws_base.clone()),
                        stream_or_id: stream_name.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });
                    stream.on("message", |_| {}).await;
                    {
                        let guard = stream.callback.lock().await;
                        assert!(guard.is_some());
                    }
                    stream.unsubscribe().await;
                    sleep(Duration::from_millis(10)).await;
                    let guard = stream.callback.lock().await;
                    assert!(guard.is_none());
                    let state = conn.state.lock().await;
                    assert!(
                        state
                            .stream_callbacks
                            .get(&stream_name)
                            .is_none_or(std::vec::Vec::is_empty)
                    );
                });
            }

            #[test]
            fn without_callback_does_nothing_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);
                    let identifier = "id1".to_string();

                    {
                        let mut stream_callbacks = ws_base.stream_callbacks.lock().await;
                        stream_callbacks.insert(identifier.clone(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: identifier.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.unsubscribe().await;

                    let stream_callbacks = ws_base.stream_callbacks.lock().await;
                    assert!(stream_callbacks.contains_key(&identifier));
                    let callbacks = stream_callbacks.get(&identifier).unwrap();
                    assert!(callbacks.is_empty());
                });
            }

            #[test]
            fn removes_registered_callback_and_clears_state_for_websocket_api() {
                TOKIO_SHARED_RT.block_on(async {
                    let ws_base = create_websocket_api(None, None, None);
                    let identifier = "id2".to_string();

                    {
                        let mut stream_callbacks = ws_base.stream_callbacks.lock().await;
                        stream_callbacks.insert(identifier.clone(), Vec::new());
                    }

                    let stream = Arc::new(WebsocketStream::<Value> {
                        websocket_base: WebsocketBase::WebsocketApi(ws_base.clone()),
                        stream_or_id: identifier.clone(),
                        url_path: None,
                        callback: Mutex::new(None),
                        id: None,
                        _phantom: PhantomData,
                    });

                    stream.on("message", |_| {}).await;

                    {
                        let stream_callbacks = ws_base.stream_callbacks.lock().await;
                        let callbacks = stream_callbacks
                            .get(&identifier)
                            .expect("Entry for 'id2' should exist");
                        assert_eq!(callbacks.len(), 1);
                    }

                    stream.unsubscribe().await;

                    {
                        let guard = stream.callback.lock().await;
                        assert!(guard.is_none());
                    }

                    {
                        let stream_callbacks = ws_base.stream_callbacks.lock().await;
                        let callbacks = stream_callbacks
                            .get(&identifier)
                            .expect("Entry for 'id2' should still exist");
                        assert!(callbacks.is_empty());
                    }
                });
            }
        }
    }

    mod create_stream_handler {
        use super::*;

        #[test]
        fn create_stream_handler_without_id_registers_stream() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);
                let stream_name = "foo".to_string();
                let handler = create_stream_handler::<serde_json::Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    None,
                    None,
                )
                .await;
                assert_eq!(handler.stream_or_id, stream_name);
                assert!(handler.id.is_none());
                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key(&stream_name));
            });
        }

        #[test]
        fn create_stream_handler_with_custom_string_id_registers_stream_and_id() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);
                let stream_name = "bar".to_string();
                let custom_id = StreamId::from("my-custom-id".to_string());
                let handler = create_stream_handler::<serde_json::Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    Some(custom_id.clone()),
                    None,
                )
                .await;
                assert_eq!(handler.stream_or_id, stream_name);
                assert_eq!(handler.id, Some(custom_id));
                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key(&stream_name));
            });
        }

        #[test]
        fn create_stream_handler_with_custom_integer_id_registers_stream_and_id() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);
                let stream_name = "bar".to_string();
                let custom_id = StreamId::from(123u32);
                let handler = create_stream_handler::<serde_json::Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    Some(custom_id.clone()),
                    None,
                )
                .await;
                assert_eq!(handler.stream_or_id, stream_name);
                assert_eq!(handler.id, Some(custom_id));
                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key(&stream_name));
            });
        }

        #[test]
        fn create_stream_handler_without_id_registers_api_stream() {
            TOKIO_SHARED_RT.block_on(async {
                let ws_base = create_websocket_api(None, None, None);
                let identifier = "foo-api".to_string();

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketApi(ws_base.clone()),
                    identifier.clone(),
                    None,
                    None,
                )
                .await;

                assert_eq!(handler.stream_or_id, identifier);
                assert!(handler.id.is_none());
            });
        }

        #[test]
        fn create_stream_handler_with_custom_string_id_registers_api_stream_and_id() {
            TOKIO_SHARED_RT.block_on(async {
                let ws_base = create_websocket_api(None, None, None);
                let identifier = "bar-api".to_string();
                let custom_id = StreamId::from("custom-123".to_string());

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketApi(ws_base.clone()),
                    identifier.clone(),
                    Some(custom_id.clone()),
                    None,
                )
                .await;

                assert_eq!(handler.stream_or_id, identifier);
                assert_eq!(handler.id, Some(custom_id));
            });
        }

        #[test]
        fn create_stream_handler_with_custom_integer_id_registers_api_stream_and_id() {
            TOKIO_SHARED_RT.block_on(async {
                let ws_base = create_websocket_api(None, None, None);
                let identifier = "bar-api".to_string();
                let custom_id = StreamId::from(123u32);

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketApi(ws_base.clone()),
                    identifier.clone(),
                    Some(custom_id.clone()),
                    None,
                )
                .await;

                assert_eq!(handler.stream_or_id, identifier);
                assert_eq!(handler.id, Some(custom_id));
            });
        }

        #[test]
        fn websocket_streams_without_url_path_registers_stream_key() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);
                let stream_name = "foo".to_string();

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    None,
                    None,
                )
                .await;

                assert_eq!(handler.stream_or_id, stream_name);
                assert!(handler.id.is_none());

                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key("foo"));
            });
        }

        #[test]
        fn websocket_streams_with_url_path_registers_prefixed_stream_key() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);

                {
                    let conn = ws.common.connection_pool[0].clone();
                    let mut st = conn.state.lock().await;
                    st.url_path = Some("path1".to_string());
                }

                let stream_name = "foo".to_string();

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    None,
                    Some("path1".to_string()),
                )
                .await;

                assert_eq!(handler.stream_or_id, stream_name);
                assert!(handler.id.is_none());

                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key("path1::foo"));
            });
        }

        #[test]
        fn websocket_streams_with_custom_id_preserves_id_and_registers_prefixed_key() {
            TOKIO_SHARED_RT.block_on(async {
                let ws = create_websocket_streams(Some("ws://example.com"), None, None);

                {
                    let conn = ws.common.connection_pool[0].clone();
                    let mut st = conn.state.lock().await;
                    st.url_path = Some("path1".to_string());
                }

                let stream_name = "bar".to_string();
                let custom_id = StreamId::from("my-custom-id".to_string());

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketStreams(ws.clone()),
                    stream_name.clone(),
                    Some(custom_id.clone()),
                    Some("path1".to_string()),
                )
                .await;

                assert_eq!(handler.stream_or_id, stream_name);
                assert_eq!(handler.id, Some(custom_id));

                let map = ws.connection_streams.lock().await;
                assert!(map.contains_key("path1::bar"));
            });
        }

        #[test]
        fn websocket_api_does_not_register_stream_in_connection_map() {
            TOKIO_SHARED_RT.block_on(async {
                let ws_base = create_websocket_api(None, None, None);
                let identifier = "foo-api".to_string();

                let handler = create_stream_handler::<Value>(
                    WebsocketBase::WebsocketApi(ws_base.clone()),
                    identifier.clone(),
                    None,
                    Some("path1".to_string()),
                )
                .await;

                assert_eq!(handler.stream_or_id, identifier);
                assert!(handler.id.is_none());
            });
        }
    }

    mod websocket_connection_failure_reason {
        use super::*;
        use std::io::{Error as IoError, ErrorKind};
        use tokio_tungstenite::tungstenite::Error as TungsteniteError;

        #[test]
        fn from_tungstenite_error_classifies_connection_closed() {
            let error = TungsteniteError::ConnectionClosed;
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ConnectionReset
            ));
            assert!(reason.should_reconnect());
        }

        #[test]
        fn from_tungstenite_error_classifies_already_closed() {
            let error = TungsteniteError::AlreadyClosed;
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ConnectionReset
            ));
            assert!(reason.should_reconnect());
        }

        #[test]
        fn from_tungstenite_error_classifies_io_errors() {
            // Test ConnectionReset
            let io_error = IoError::new(ErrorKind::ConnectionReset, "connection reset");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ConnectionReset
            ));
            assert!(reason.should_reconnect());

            // Test ConnectionAborted
            let io_error = IoError::new(ErrorKind::ConnectionAborted, "connection aborted");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ConnectionReset
            ));
            assert!(reason.should_reconnect());

            // Test TimedOut
            let io_error = IoError::new(ErrorKind::TimedOut, "timed out");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::NetworkInterruption
            ));
            assert!(reason.should_reconnect());

            // Test UnexpectedEof
            let io_error = IoError::new(ErrorKind::UnexpectedEof, "unexpected eof");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::StreamEnded
            ));
            assert!(reason.should_reconnect());

            // Test PermissionDenied
            let io_error = IoError::new(ErrorKind::PermissionDenied, "permission denied");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::AuthenticationFailure
            ));
            assert!(!reason.should_reconnect());

            // Test other IO errors default to NetworkInterruption
            let io_error = IoError::other("other error");
            let error = TungsteniteError::Io(io_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::NetworkInterruption
            ));
            assert!(reason.should_reconnect());
        }

        #[test]
        fn from_tungstenite_error_classifies_protocol_errors() {
            // Protocol error -> ProtocolViolation
            use tokio_tungstenite::tungstenite::error::ProtocolError;
            let protocol_error = ProtocolError::ResetWithoutClosingHandshake;
            let error = TungsteniteError::Protocol(protocol_error);
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UnexpectedClose
            ));
            assert!(reason.should_reconnect());

            // UTF8 error -> ProtocolViolation
            let error = TungsteniteError::Utf8;
            let reason = WebsocketConnectionFailureReason::from_tungstenite_error(&error);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ProtocolViolation
            ));
            assert!(!reason.should_reconnect());
        }

        #[test]
        fn from_close_code_classifies_standard_codes() {
            // Normal closure
            let reason = WebsocketConnectionFailureReason::from_close_code(1000, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::NormalClose
            ));
            assert!(!reason.should_reconnect());

            // Going away (server restart) -> ServerTemporaryError
            let reason = WebsocketConnectionFailureReason::from_close_code(1001, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ServerTemporaryError
            ));
            assert!(reason.should_reconnect());

            // Protocol error -> ProtocolViolation
            let reason = WebsocketConnectionFailureReason::from_close_code(1002, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ProtocolViolation
            ));
            assert!(!reason.should_reconnect());

            // Abnormal closure -> UnexpectedClose
            let reason = WebsocketConnectionFailureReason::from_close_code(1006, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UnexpectedClose
            ));
            assert!(reason.should_reconnect());

            // Policy violation -> PermanentServerError
            let reason = WebsocketConnectionFailureReason::from_close_code(1008, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::PermanentServerError
            ));
            assert!(!reason.should_reconnect());

            // Server error -> ServerTemporaryError
            let reason = WebsocketConnectionFailureReason::from_close_code(1011, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ServerTemporaryError
            ));
            assert!(reason.should_reconnect());

            // TLS handshake failure -> ConfigurationError
            let reason = WebsocketConnectionFailureReason::from_close_code(1015, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::ConfigurationError
            ));
            assert!(!reason.should_reconnect());

            // Business/application errors -> PermanentServerError
            let reason = WebsocketConnectionFailureReason::from_close_code(4000, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::PermanentServerError
            ));
            assert!(!reason.should_reconnect());

            let reason = WebsocketConnectionFailureReason::from_close_code(4999, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::PermanentServerError
            ));
            assert!(!reason.should_reconnect());

            // Unknown codes default to UnexpectedClose
            let reason = WebsocketConnectionFailureReason::from_close_code(9999, false);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UnexpectedClose
            ));
            assert!(reason.should_reconnect());
        }

        #[test]
        fn from_close_code_handles_user_initiated() {
            // Any code with user_initiated=true should return UserInitiatedClose
            let reason = WebsocketConnectionFailureReason::from_close_code(1000, true);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UserInitiatedClose
            ));
            assert!(!reason.should_reconnect());

            let reason = WebsocketConnectionFailureReason::from_close_code(1006, true);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UserInitiatedClose
            ));
            assert!(!reason.should_reconnect());

            let reason = WebsocketConnectionFailureReason::from_close_code(4000, true);
            assert!(matches!(
                reason,
                WebsocketConnectionFailureReason::UserInitiatedClose
            ));
            assert!(!reason.should_reconnect());
        }

        #[test]
        fn should_reconnect_logic() {
            // Reconnectable failures
            assert!(WebsocketConnectionFailureReason::NetworkInterruption.should_reconnect());
            assert!(WebsocketConnectionFailureReason::ConnectionReset.should_reconnect());
            assert!(WebsocketConnectionFailureReason::ServerTemporaryError.should_reconnect());
            assert!(WebsocketConnectionFailureReason::UnexpectedClose.should_reconnect());
            assert!(WebsocketConnectionFailureReason::StreamEnded.should_reconnect());

            // Non-reconnectable failures
            assert!(!WebsocketConnectionFailureReason::AuthenticationFailure.should_reconnect());
            assert!(!WebsocketConnectionFailureReason::ProtocolViolation.should_reconnect());
            assert!(!WebsocketConnectionFailureReason::ConfigurationError.should_reconnect());
            assert!(!WebsocketConnectionFailureReason::UserInitiatedClose.should_reconnect());
            assert!(!WebsocketConnectionFailureReason::PermanentServerError.should_reconnect());
            assert!(!WebsocketConnectionFailureReason::NormalClose.should_reconnect());
        }

        #[test]
        fn debug_and_clone_work() {
            let reason = WebsocketConnectionFailureReason::NetworkInterruption;
            let cloned = reason;
            let debug_str = format!("{:?}", reason);

            assert!(matches!(
                cloned,
                WebsocketConnectionFailureReason::NetworkInterruption
            ));
            assert!(debug_str.contains("NetworkInterruption"));
        }
    }
}
