use derive_builder::UninitializedFieldError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::marker::PhantomData;
use std::{collections::HashMap, future::Future, pin::Pin};
use thiserror::Error;

use super::errors::ConnectorError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Millisecond,
    Microsecond,
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::Millisecond => write!(f, "millisecond"),
            TimeUnit::Microsecond => write!(f, "microsecond"),
        }
    }
}

impl TimeUnit {
    #[must_use]
    pub fn as_upper_str(&self) -> &'static str {
        match self {
            TimeUnit::Millisecond => "MILLISECOND",
            TimeUnit::Microsecond => "MICROSECOND",
        }
    }
    #[must_use]
    pub fn as_lower_str(&self) -> &'static str {
        match self {
            TimeUnit::Millisecond => "millisecond",
            TimeUnit::Microsecond => "microsecond",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RateLimitType {
    RequestWeight,
    Orders,
    RawRequests,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Interval {
    Second,
    Minute,
    Hour,
    Day,
}

#[derive(Debug, Clone)]
pub struct RestApiRateLimit {
    pub rate_limit_type: RateLimitType,
    pub interval: Interval,
    pub interval_num: u32,
    pub count: u32,
    pub retry_after: Option<u32>,
}

pub type DataFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

pub struct RestApiResponse<T> {
    pub(crate) data_fn: Box<
        dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<T, ConnectorError>> + Send>>
            + Send
            + Sync,
    >,
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub rate_limits: Option<Vec<RestApiRateLimit>>,
}

impl<T> RestApiResponse<T>
where
    T: Send + 'static,
{
    /// Executes the data retrieval function and returns the result.
    ///
    /// # Returns
    ///
    /// A `Result` containing the data of type `T` if successful,
    /// or a `ConnectorError` if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    ///
    /// # Examples
    ///
    ///
    /// let response: `RestApiResponse`<MyType> = ...;
    /// let data = response.data().await?;
    ///
    pub async fn data(self) -> Result<T, ConnectorError> {
        (self.data_fn)().await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WebsocketApiRateLimit {
    #[serde(rename = "rateLimitType")]
    pub rate_limit_type: RateLimitType,
    #[serde(rename = "interval")]
    pub interval: Interval,
    #[serde(rename = "intervalNum")]
    pub interval_num: u32,
    pub limit: u32,
    #[serde(default)]
    pub count: u32,
}

#[derive(Debug)]
pub struct WebsocketApiResponse<T> {
    pub(crate) _marker: PhantomData<T>,

    pub raw: Value,
    pub rate_limits: Option<Vec<WebsocketApiRateLimit>>,
}

impl<T> WebsocketApiResponse<T>
where
    T: DeserializeOwned,
{
    /// Deserializes the raw JSON value into the generic type `T`.
    ///
    /// # Returns
    ///
    /// A `Result` containing the deserialized value of type `T` if successful,
    /// or a `serde_json::Error` if deserialization fails.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    ///
    /// # Examples
    ///
    ///
    /// // Assuming `WebsocketApiResponse` contains a raw JSON value
    /// let response: `WebsocketApiResponse`<MyType> = ...;
    /// let data = `response.data()`?;
    ///
    pub fn data(self) -> serde_json::Result<T> {
        serde_json::from_value(self.raw)
    }
}

#[derive(Debug, Error)]
pub enum ParamBuildError {
    #[error("missing required field `{0}`")]
    UninitializedField(&'static str),
}

impl From<UninitializedFieldError> for ParamBuildError {
    fn from(err: UninitializedFieldError) -> Self {
        ParamBuildError::UninitializedField(err.field_name())
    }
}

#[derive(Debug, Error)]
pub enum ConfigBuildError {
    #[error("Configuration missing or invalid `{0}`")]
    UninitializedField(&'static str),
}

impl From<UninitializedFieldError> for ConfigBuildError {
    fn from(err: UninitializedFieldError) -> Self {
        ConfigBuildError::UninitializedField(err.field_name())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WebsocketEvent {
    Open,
    /// Raw server frame. It may contain credentials such as private listen
    /// keys; treat it as secret-bearing data and never log it verbatim.
    Message(String),
    Error(String),
    Close(u16, String),
    Ping,
    Pong,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WebsocketMode {
    Single,
    Pool(usize),
}

impl WebsocketMode {
    #[must_use]
    pub fn pool_size(&self) -> usize {
        match *self {
            WebsocketMode::Single => 1,
            WebsocketMode::Pool(sz) => sz,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct WebsocketApiConnectConfig {
    pub mode: Option<WebsocketMode>,
}

#[derive(Debug, Clone, Default)]
pub struct WebsocketStreamsConnectConfig {
    pub streams: Vec<String>,
    pub mode: Option<WebsocketMode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamId {
    Str(String),
    Number(u32),
}

/// Secret-free generated endpoint scope for a stream-subscription request.
///
/// Arbitrary internal path strings are collapsed to [`Self::Other`] so a
/// caller-controlled path, query, or credential can never enter diagnostic
/// contexts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSubscriptionScope {
    Default,
    Market,
    Public,
    Private,
    Stream,
    Ws,
    WsApi,
    Other,
}

/// Routed USD-M public stream endpoint accepted by confirmed JSON control
/// requests.
///
/// The closed enum prevents a caller-controlled path, URL, or query string
/// from becoming part of transport selection or diagnostics.
#[cfg(feature = "derivatives_trading_usds_futures")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutedStreamScope {
    Public,
    Market,
}

#[cfg(feature = "derivatives_trading_usds_futures")]
impl RoutedStreamScope {
    /// Returns the generated routed path used by the USD-M stream transport.
    #[must_use]
    pub fn as_path_scope(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Market => "market",
        }
    }

    /// Parses a generated, already-redacted lifecycle path scope.
    #[must_use]
    pub fn from_path_scope(scope: Option<&str>) -> Option<Self> {
        match scope {
            Some("public") => Some(Self::Public),
            Some("market") => Some(Self::Market),
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn subscription_scope(self) -> StreamSubscriptionScope {
        match self {
            Self::Public => StreamSubscriptionScope::Public,
            Self::Market => StreamSubscriptionScope::Market,
        }
    }
}

/// Exact physical routed WebSocket session on which to dispatch a confirmed
/// public stream control request.
///
/// Construct this target from an `Open` lifecycle observation. A target stops
/// being valid when the same connection slot opens a newer
/// `session_generation`.
#[cfg(feature = "derivatives_trading_usds_futures")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedStreamTarget {
    pub connection_id: String,
    pub session_generation: u64,
    pub path_scope: RoutedStreamScope,
}

#[cfg(feature = "derivatives_trading_usds_futures")]
impl RoutedStreamTarget {
    #[must_use]
    pub fn new(
        connection_id: impl Into<String>,
        session_generation: u64,
        path_scope: RoutedStreamScope,
    ) -> Self {
        Self {
            connection_id: connection_id.into(),
            session_generation,
            path_scope,
        }
    }

    #[must_use]
    pub(crate) fn subscription_context(&self, request_id: u32) -> StreamSubscriptionContext {
        StreamSubscriptionContext {
            connection_id: self.connection_id.clone(),
            session_generation: self.session_generation,
            request_id,
            path_scope: self.path_scope.subscription_scope(),
        }
    }
}

/// Identifies one JSON stream-subscription request on one physical WebSocket
/// session. It intentionally contains no stream name: private stream names may
/// contain a listen key and must not leak through diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSubscriptionContext {
    pub connection_id: String,
    pub session_generation: u64,
    pub request_id: u32,
    /// Closed, redacted generated endpoint scope; never an arbitrary path,
    /// query string, stream parameter, or full URL.
    pub path_scope: StreamSubscriptionScope,
}

/// Result observed for a correlated JSON stream-subscription request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamSubscriptionOutcome {
    Dispatched,
    Acknowledged,
    Rejected { code: i64 },
    TimedOut,
    DispatchFailed,
    ProtocolError,
    SessionReplaced,
    Disconnected,
    Cancelled,
}

/// Low-level stream-subscription lifecycle event. Stream parameters are never
/// included because a private stream parameter can be a listen key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSubscriptionEvent {
    pub context: StreamSubscriptionContext,
    pub outcome: StreamSubscriptionOutcome,
}

/// Successful, server-confirmed JSON stream subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSubscriptionAck {
    pub context: StreamSubscriptionContext,
}

impl From<String> for StreamId {
    fn from(v: String) -> Self {
        StreamId::Str(v)
    }
}
impl From<u32> for StreamId {
    fn from(v: u32) -> Self {
        StreamId::Number(v)
    }
}
