use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use lettre::address::AddressError;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum CRRError {
    #[error("Database Error: {0}")]
    DatabaseError(#[from] rusqlite::Error),
    #[error("Parser Error: {0}")]
    ParserError(#[from] std::string::FromUtf8Error),
    #[error("SMTP Error: {0}")]
    SmtpError(#[from] lettre::transport::smtp::Error),
    #[error("Mailing Error: {0}")]
    MailingError(#[from] lettre::error::Error),
    #[error("Invalid email-Address: {0}")]
    InvalidAddress(#[from] AddressError),
    #[error("Environment Error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    //#[error("Message Passing Error: {0}")]
    //BroadcastSendError(#[from] tokio::sync::broadcast::error::SendError<ChangeMessage>),
    #[error("Message Passing Error: {0}")]
    BroadcastRecvError(#[from] tokio::sync::broadcast::error::RecvError),
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Validation Error: {0}")]
    ValidationError(String),
    #[error("Type Error: Array-typed parameters are currently not supported")]
    ParameterArrayTypeError,
    #[error("Type Error: Object-typed parameters are currently not supported")]
    ParameterObjectTypeError,
    #[error("Type Error: Failed to deserialize number")]
    ParameterNumberTypeError,
    #[error("Unsupported OS: {0}")]
    UnsupportedOS(String),
}

impl IntoResponse for CRRError {
    fn into_response(self) -> Response {
        tracing::error!("{}", self.to_string());

        match self {
            Self::Unauthorized(message) => (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "message": &message })),
            ),

            Self::ValidationError(message) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "message": &message })),
            ),

            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "Unexpected Error"})),
            ),
        }
        .into_response()
    }
}

impl CRRError {
    pub(crate) fn unauthorized(msg: String) -> Self {
        Self::Unauthorized(msg)
    }
}
