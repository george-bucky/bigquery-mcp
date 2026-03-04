use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug, Error, Clone)]
pub enum AppError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("rate limited: {0}")]
    RateLimited(String),
    #[error("upstream timeout: {0}")]
    UpstreamTimeout(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl AppError {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Unauthenticated(_) => "UNAUTHENTICATED",
            Self::PermissionDenied(_) => "PERMISSION_DENIED",
            Self::RateLimited(_) => "RATE_LIMITED",
            Self::UpstreamTimeout(_) => "UPSTREAM_TIMEOUT",
            Self::Transport(_) => "INTERNAL",
            Self::Serialization(_) => "INTERNAL",
            Self::Internal(_) => "INTERNAL",
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(
            self,
            Self::RateLimited(_) | Self::UpstreamTimeout(_) | Self::Transport(_)
        )
    }

    pub fn jsonrpc_code(&self) -> i64 {
        match self {
            Self::InvalidArgument(_) => -32602,
            Self::NotFound(_) => -32004,
            Self::Unauthenticated(_) => -32001,
            Self::PermissionDenied(_) => -32003,
            Self::RateLimited(_) => -32029,
            Self::UpstreamTimeout(_) => -32008,
            Self::Transport(_) | Self::Serialization(_) | Self::Internal(_) => -32603,
        }
    }
}
