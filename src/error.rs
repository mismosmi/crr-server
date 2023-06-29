use std::{backtrace::Backtrace, convert::Infallible};

use axum::{
    extract::rejection::PathRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use lettre::address::AddressError;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CRRError {
    #[error("Database Error: {0}\n{1}")]
    DatabaseError(#[from] rusqlite::Error, Backtrace),
    #[error("Parser Error: {0}")]
    ParserError(#[from] std::string::FromUtf8Error),
    #[error("Invalid URL: {0}")]
    InvalidURLError(#[from] url::ParseError),
    #[error("SMTP Error: {0}")]
    SmtpError(#[from] lettre::transport::smtp::Error),
    #[error("Mailing Error: {0}")]
    MailingError(#[from] lettre::error::Error),
    #[error("Invalid email-Address: {0}")]
    InvalidAddress(#[from] AddressError),
    #[error("Environment Error: {0}")]
    EnvVarError(#[from] std::env::VarError),
    #[error("Message Passing Error: {0}")]
    BroadcastRecvError(#[from] tokio::sync::broadcast::error::RecvError),
    #[error("IO Error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[error("Unsupported OS: {0}")]
    UnsupportedOS(String),
    #[error("Poisoned Lock Error in {0}")]
    PoisonedLockError(&'static str),
    #[error("Message Passing Error: {0}")]
    SignalSendError(#[from] tokio::sync::mpsc::error::SendError<()>),
    #[error("JSON Error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Database {0} is reserved for Internal Purposes")]
    ReservedName(String),
    #[error("Invalid Path Parameter: {0}")]
    PathRejection(#[from] PathRejection),
    #[error("Failed to decode Base64-String: {0}")]
    Base64DecodeError(#[from] base64::DecodeError),
}

impl From<Infallible> for CRRError {
    fn from(_value: Infallible) -> Self {
        unreachable!()
    }
}

#[derive(Error, Debug, Clone)]
#[error("{status_code}: {message}")]
pub(crate) struct HttpError {
    status_code: StatusCode,
    message: String,
}

impl From<CRRError> for HttpError {
    fn from(value: CRRError) -> Self {
        tracing::error!("{}", value);

        match value {
            CRRError::Unauthorized(message) => Self {
                status_code: StatusCode::UNAUTHORIZED,
                message,
            },
            _ => Self {
                status_code: StatusCode::INTERNAL_SERVER_ERROR,
                message: "Internal Server Error".to_owned(),
            },
        }
    }
}

impl HttpError {
    fn status_code(&self) -> StatusCode {
        self.status_code
    }

    fn message(&self) -> &str {
        &self.message
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (
            self.status_code(),
            Json(json!({ "message": self.message() })),
        )
            .into_response()
    }
}

impl IntoResponse for CRRError {
    fn into_response(self) -> Response {
        let http_error: HttpError = self.into();

        http_error.into_response()
    }
}

impl CRRError {
    pub(crate) fn unauthorized(msg: String) -> Self {
        Self::Unauthorized(msg)
    }
}
