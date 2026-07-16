use derive_builder::Builder;
use reqwest::{Client, ClientBuilder};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio_tungstenite::Connector;

use super::models::{ConfigBuildError, StreamSubscriptionEvent, TimeUnit, WebsocketMode};
use super::utils::{SignatureGenerator, build_client_with_redirects};

#[derive(Clone)]
pub struct AgentConnector(pub Connector);

impl fmt::Debug for AgentConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connector(…)")
    }
}

#[derive(Clone)]
pub struct HttpAgent(pub Arc<dyn Fn(ClientBuilder) -> ClientBuilder + Send + Sync>);

impl fmt::Debug for HttpAgent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HttpAgent(<custom agent fn>)")
    }
}

/// Native WebSocket data-message kind presented to a context-aware raw observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawFrameKind {
    Text,
    Binary,
}

/// Borrowed metadata for one raw WebSocket data message.
///
/// `path_scope` is the SDK's generated endpoint scope (for example `market`,
/// `public`, or `private`); arbitrary internal values are reported as `other`,
/// never as the full URL or query string. The payload is
/// observed before generated JSON/deserialization handling and, for binary
/// messages, before decompression. Text has already passed the WebSocket
/// implementation's protocol and UTF-8 validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFrameContext<'a> {
    pub connection_id: &'a str,
    /// Monotonic physical-session generation for this pool slot.
    pub session_generation: u64,
    pub path_scope: Option<&'a str>,
    pub kind: RawFrameKind,
    pub payload: &'a [u8],
}

/// Scoped WebSocket transport lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebsocketLifecycleEvent {
    Open,
    Ping,
    Pong,
    Close { code: u16 },
    ReadError,
    WriteError,
    ReconnectScheduled { is_renewal: bool },
    StreamEnded,
}

/// Borrowed metadata for a WebSocket lifecycle transition.
///
/// No full URL or error text is exposed because private stream URLs can carry
/// credentials. Consumers can correlate the stable connection id and the
/// generated path scope without receiving secret-bearing transport strings.
/// Arbitrary internal values are collapsed to `other`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebsocketLifecycleContext<'a> {
    pub connection_id: &'a str,
    /// Monotonic physical-session generation for this pool slot.
    pub session_generation: u64,
    pub path_scope: Option<&'a str>,
    pub event: WebsocketLifecycleEvent,
}

type LegacyRawFrameObserver = dyn Fn(&str, &[u8]) + Send + Sync;
type ContextRawFrameObserver = dyn for<'a> Fn(RawFrameContext<'a>) + Send + Sync;
type LifecycleObserver = dyn for<'a> Fn(WebsocketLifecycleContext<'a>) + Send + Sync;

/// Synchronous observer for WebSocket raw frames and scoped lifecycle events.
///
/// [`Self::new`] preserves the original data-frame-only callback. New
/// integrations should use [`Self::new_context`] and optionally
/// [`Self::with_lifecycle`] so one SDK instance can safely distinguish
/// path-scoped connections, frame encoding, heartbeat, and reconnects.
/// Keep callbacks non-blocking and non-panicking; a typical callback copies the
/// borrowed data into an application-owned queue or ring buffer. Panics are
/// caught at this SDK boundary so they cannot terminate transport actors.
#[derive(Clone)]
pub struct RawFrameObserver {
    legacy: Option<Arc<LegacyRawFrameObserver>>,
    context: Option<Arc<ContextRawFrameObserver>>,
    lifecycle: Option<Arc<LifecycleObserver>>,
}

impl RawFrameObserver {
    #[must_use]
    pub fn new<F>(observer: F) -> Self
    where
        F: Fn(&str, &[u8]) + Send + Sync + 'static,
    {
        Self {
            legacy: Some(Arc::new(observer)),
            context: None,
            lifecycle: None,
        }
    }

    /// Creates a path- and encoding-aware pre-decode observer.
    #[must_use]
    pub fn new_context<F>(observer: F) -> Self
    where
        F: for<'a> Fn(RawFrameContext<'a>) + Send + Sync + 'static,
    {
        Self {
            legacy: None,
            context: Some(Arc::new(observer)),
            lifecycle: None,
        }
    }

    /// Adds a scoped lifecycle observer to either raw-frame constructor.
    #[must_use]
    pub fn with_lifecycle<F>(mut self, observer: F) -> Self
    where
        F: for<'a> Fn(WebsocketLifecycleContext<'a>) + Send + Sync + 'static,
    {
        self.lifecycle = Some(Arc::new(observer));
        self
    }

    pub(crate) fn observe_frame(&self, context: RawFrameContext<'_>) {
        if let Some(observer) = &self.legacy {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer(context.connection_id, context.payload);
            }))
            .is_err()
            {
                tracing::error!(
                    "Raw WebSocket observer panicked on connection {}",
                    context.connection_id
                );
            }
        }
        if let Some(observer) = &self.context {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer(context);
            }))
            .is_err()
            {
                tracing::error!(
                    "Context WebSocket observer panicked on connection {}",
                    context.connection_id
                );
            }
        }
    }

    pub(crate) fn observe_lifecycle(&self, context: WebsocketLifecycleContext<'_>) {
        if let Some(observer) = &self.lifecycle {
            if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                observer(context);
            }))
            .is_err()
            {
                tracing::error!(
                    "Lifecycle WebSocket observer panicked on connection {}",
                    context.connection_id
                );
            }
        }
    }
}

impl fmt::Debug for RawFrameObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawFrameObserver(<custom observer fn>)")
    }
}

type StreamSubscriptionObserverFn = dyn Fn(&StreamSubscriptionEvent) + Send + Sync;

/// Synchronous, redacted observer for one stream-subscription lifecycle.
///
/// The callback runs inline at the transport boundary to preserve causal
/// ordering with raw frames. Keep it non-blocking. Panics are caught so an
/// application observer cannot terminate WebSocket transport actors.
#[derive(Clone)]
pub struct StreamSubscriptionObserver(Arc<StreamSubscriptionObserverFn>);

impl StreamSubscriptionObserver {
    #[must_use]
    pub fn new<F>(observer: F) -> Self
    where
        F: Fn(&StreamSubscriptionEvent) + Send + Sync + 'static,
    {
        Self(Arc::new(observer))
    }

    pub(crate) fn observe(&self, event: &StreamSubscriptionEvent) {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self.0)(event);
        }))
        .is_err()
        {
            tracing::error!(
                "Stream subscription observer panicked on connection {} generation {} request {}",
                event.context.connection_id,
                event.context.session_generation,
                event.context.request_id
            );
        }
    }
}

impl fmt::Debug for StreamSubscriptionObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamSubscriptionObserver(<custom observer fn>)")
    }
}

#[derive(Clone)]
pub struct ProxyAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    pub protocol: Option<String>,
    pub auth: Option<ProxyAuth>,
}

impl fmt::Debug for ProxyAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyAuth")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub enum PrivateKey {
    File(String),
    Raw(Vec<u8>),
}

impl fmt::Debug for PrivateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PrivateKey::File(_) => write!(f, "PrivateKey::File([REDACTED])"),
            PrivateKey::Raw(_) => write!(f, "PrivateKey::Raw([REDACTED])"),
        }
    }
}

#[derive(Clone, Builder)]
#[builder(
    pattern = "owned",
    build_fn(name = "try_build", error = "ConfigBuildError")
)]
pub struct ConfigurationRestApi {
    #[builder(setter(into, strip_option), default)]
    pub api_key: Option<String>,

    #[builder(setter(into, strip_option), default)]
    pub api_secret: Option<String>,

    #[builder(setter(into, strip_option), default)]
    pub base_path: Option<String>,

    #[builder(default = "1000")]
    pub timeout: u64,

    #[builder(default = "true")]
    pub keep_alive: bool,

    #[builder(default = "true")]
    pub compression: bool,

    /// Whether the REST client follows HTTP redirects. Disable this for
    /// state-changing signed requests to prevent replay at a redirect target.
    #[builder(default = "true")]
    pub follow_redirects: bool,

    #[builder(default = "3")]
    pub retries: u32,

    /// Preserve HTTP status, Binance code/message, and response headers in a
    /// single `ConnectorError::ApiError`. This is useful for trading clients
    /// that must normalize venue errors without losing `Retry-After`.
    #[builder(default = "false")]
    pub preserve_error_response: bool,

    #[builder(default = "1000")]
    pub backoff: u64,

    #[builder(setter(strip_option), default)]
    pub proxy: Option<ProxyConfig>,

    #[builder(setter(strip_option, into), default)]
    pub custom_headers: Option<HashMap<String, String>>,

    #[builder(setter(strip_option), default)]
    pub agent: Option<HttpAgent>,

    #[builder(setter(strip_option), default)]
    pub private_key: Option<PrivateKey>,

    #[builder(setter(strip_option), default)]
    pub private_key_passphrase: Option<String>,

    #[builder(setter(strip_option), default)]
    pub time_unit: Option<TimeUnit>,

    #[builder(setter(skip))]
    pub(crate) client: Client,

    #[builder(setter(skip))]
    pub(crate) user_agent: String,

    #[builder(setter(skip))]
    pub(crate) signature_gen: SignatureGenerator,
}

impl fmt::Debug for ConfigurationRestApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigurationRestApi")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("base_path", &self.base_path)
            .field("timeout", &self.timeout)
            .field("keep_alive", &self.keep_alive)
            .field("compression", &self.compression)
            .field("follow_redirects", &self.follow_redirects)
            .field("retries", &self.retries)
            .field("preserve_error_response", &self.preserve_error_response)
            .field("backoff", &self.backoff)
            .field("proxy", &self.proxy)
            .field("custom_headers", &self.custom_headers)
            .field("agent", &self.agent)
            .field(
                "private_key",
                &self.private_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "private_key_passphrase",
                &self.private_key_passphrase.as_ref().map(|_| "[REDACTED]"),
            )
            .field("time_unit", &self.time_unit)
            .field("client", &"<reqwest::Client>")
            .field("user_agent", &self.user_agent)
            .field("signature_gen", &self.signature_gen)
            .finish()
    }
}

impl ConfigurationRestApi {
    #[must_use]
    pub fn builder() -> ConfigurationRestApiBuilder {
        ConfigurationRestApiBuilder::default()
    }
}

impl ConfigurationRestApiBuilder {
    /// Builds a `ConfigurationRestApi` instance with configured HTTP client and signature generator.
    ///
    /// # Returns
    ///
    /// A `Result` containing the fully configured `ConfigurationRestApi` or a `ConfigBuildError` if configuration fails.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigBuildError` if the initial configuration build fails or if client setup encounters issues.
    pub fn build(self) -> Result<ConfigurationRestApi, ConfigBuildError> {
        let mut cfg = self.try_build()?;
        cfg.client = build_client_with_redirects(
            cfg.timeout,
            cfg.keep_alive,
            cfg.proxy.as_ref(),
            cfg.agent.clone(),
            cfg.follow_redirects,
        );
        cfg.signature_gen = SignatureGenerator::new(
            cfg.api_secret.clone(),
            cfg.private_key.clone(),
            cfg.private_key_passphrase.clone(),
        );

        Ok(cfg)
    }
}

#[derive(Clone, Builder)]
#[builder(
    pattern = "owned",
    build_fn(name = "try_build", error = "ConfigBuildError")
)]
pub struct ConfigurationWebsocketApi {
    #[builder(setter(into, strip_option), default)]
    pub api_key: Option<String>,

    #[builder(setter(into, strip_option), default)]
    pub api_secret: Option<String>,

    #[builder(setter(into, strip_option), default)]
    pub ws_url: Option<String>,

    #[builder(default = "5000")]
    pub timeout: u64,

    #[builder(default = "5000")]
    pub reconnect_delay: u64,

    #[builder(default = "WebsocketMode::Single")]
    pub mode: WebsocketMode,

    #[builder(setter(strip_option), default)]
    pub agent: Option<AgentConnector>,

    #[builder(setter(strip_option), default)]
    pub private_key: Option<PrivateKey>,

    #[builder(setter(strip_option), default)]
    pub private_key_passphrase: Option<String>,

    #[builder(setter(strip_option), default)]
    pub time_unit: Option<TimeUnit>,

    #[builder(default = "true")]
    pub auto_session_relogon: bool,

    #[builder(setter(skip))]
    pub(crate) user_agent: String,

    #[builder(setter(skip))]
    pub(crate) signature_gen: SignatureGenerator,
}

impl fmt::Debug for ConfigurationWebsocketApi {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConfigurationWebsocketApi")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "[REDACTED]"),
            )
            .field("ws_url", &self.ws_url)
            .field("timeout", &self.timeout)
            .field("reconnect_delay", &self.reconnect_delay)
            .field("mode", &self.mode)
            .field("agent", &self.agent)
            .field(
                "private_key",
                &self.private_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "private_key_passphrase",
                &self.private_key_passphrase.as_ref().map(|_| "[REDACTED]"),
            )
            .field("time_unit", &self.time_unit)
            .field("auto_session_relogon", &self.auto_session_relogon)
            .field("user_agent", &self.user_agent)
            .field("signature_gen", &self.signature_gen)
            .finish()
    }
}

impl ConfigurationWebsocketApi {
    /// Creates a builder for `ConfigurationWebsocketApi` with the specified API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - The API key to be used for the WebSocket API configuration
    ///
    /// # Returns
    ///
    /// A `ConfigurationWebsocketApiBuilder` initialized with the provided API key
    #[must_use]
    pub fn builder() -> ConfigurationWebsocketApiBuilder {
        ConfigurationWebsocketApiBuilder::default()
    }
}

impl ConfigurationWebsocketApiBuilder {
    /// Builds the `ConfigurationWebsocketApi` with a generated signature generator.
    ///
    /// This method attempts to build the configuration using the builder's settings
    /// and then initializes the signature generator with the API secret, private key,
    /// and private key passphrase.
    ///
    /// # Returns
    ///
    /// A `Result` containing the fully configured `ConfigurationWebsocketApi` or a
    /// `ConfigBuildError` if the build process fails.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigBuildError` if the initial configuration build fails or if signature generation fails.
    ///
    pub fn build(self) -> Result<ConfigurationWebsocketApi, ConfigBuildError> {
        let mut cfg = self.try_build()?;
        cfg.signature_gen = SignatureGenerator::new(
            cfg.api_secret.clone(),
            cfg.private_key.clone(),
            cfg.private_key_passphrase.clone(),
        );

        Ok(cfg)
    }
}

#[derive(Debug, Clone, Builder)]
#[builder(pattern = "owned", build_fn(error = "ConfigBuildError"))]
pub struct ConfigurationWebsocketStreams {
    #[builder(setter(into, strip_option), default)]
    pub ws_url: Option<String>,

    #[builder(default = "5000")]
    pub reconnect_delay: u64,

    #[builder(default = "WebsocketMode::Single")]
    pub mode: WebsocketMode,

    #[builder(setter(strip_option), default)]
    pub agent: Option<AgentConnector>,

    #[builder(setter(strip_option), default)]
    pub time_unit: Option<TimeUnit>,

    /// Optional synchronous hook for raw Text/Binary payloads before decode.
    #[builder(setter(strip_option), default)]
    pub raw_frame_observer: Option<RawFrameObserver>,

    /// Optional synchronous, redacted stream-subscription lifecycle hook.
    #[builder(setter(strip_option), default)]
    pub stream_subscription_observer: Option<StreamSubscriptionObserver>,

    #[builder(setter(skip))]
    pub(crate) user_agent: String,
}

impl ConfigurationWebsocketStreams {
    #[must_use]
    /// Creates a builder for `ConfigurationWebsocketStreams` with default settings.
    ///
    /// # Returns
    ///
    /// A `ConfigurationWebsocketStreamsBuilder` initialized with default values
    pub fn builder() -> ConfigurationWebsocketStreamsBuilder {
        ConfigurationWebsocketStreamsBuilder::default()
    }
}
