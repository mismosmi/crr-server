use std::string::FromUtf8Error;

use rocket::{serde::Serialize, tokio};
use thiserror::Error;

use crate::database::ChangeMessage;

#[derive(rocket::Responder, Debug, Clone, Serialize, Error)]
#[serde(crate = "rocket::serde")]
pub(crate) enum CRRError {
    #[response(status = 500)]
    #[error("Database Error: {0}")]
    DatabaseError(String),
    #[response(status = 500)]
    #[error("Server Error: {0}")]
    ServerError(String),
    #[response(status = 500)]
    #[error("SMTP Error: {0}")]
    SmtpError(String),
    #[response(status = 500)]
    #[error("Environment Error: {0}")]
    EnvVarError(String),
    #[response(status = 401)]
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
    #[response(status = 400)]
    #[error("Validation Error: {0}")]
    ValidationError(String),
}

impl From<rusqlite::Error> for CRRError {
    fn from(value: rusqlite::Error) -> Self {
        CRRError::DatabaseError(format!("DatabaseError: {}", value))
    }
}

impl From<rocket::Error> for CRRError {
    fn from(value: rocket::Error) -> Self {
        CRRError::ServerError(value.to_string())
    }
}

impl From<lettre::transport::smtp::Error> for CRRError {
    fn from(value: lettre::transport::smtp::Error) -> Self {
        CRRError::SmtpError(format!("SMTP Error: {:?}", value))
    }
}

impl From<std::env::VarError> for CRRError {
    fn from(value: std::env::VarError) -> Self {
        CRRError::EnvVarError(value.to_string())
    }
}

impl From<dotenv::Error> for CRRError {
    fn from(value: dotenv::Error) -> Self {
        CRRError::EnvVarError(value.to_string())
    }
}

impl From<lettre::error::Error> for CRRError {
    fn from(value: lettre::error::Error) -> Self {
        CRRError::SmtpError(value.to_string())
    }
}

impl From<FromUtf8Error> for CRRError {
    fn from(_value: FromUtf8Error) -> Self {
        CRRError::ServerError("Failed to parse Text as utf8".to_owned())
    }
}

impl From<tokio::sync::broadcast::error::SendError<ChangeMessage>> for CRRError {
    fn from(_value: tokio::sync::broadcast::error::SendError<ChangeMessage>) -> Self {
        CRRError::ServerError("Internal message passing failure".to_owned())
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for CRRError {
    fn from(_value: tokio::sync::broadcast::error::RecvError) -> Self {
        CRRError::ServerError("Internal message passing failure".to_owned())
    }
}

impl From<std::io::Error> for CRRError {
    fn from(_value: std::io::Error) -> Self {
        CRRError::ServerError("IO Error".to_owned())
    }
}
