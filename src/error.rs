use std::string::FromUtf8Error;

use rocket::{serde::Serialize, tokio};

use crate::database::ChangeMessage;

#[derive(rocket::Responder, Debug, Clone, Serialize)]
#[serde(crate = "rocket::serde")]
pub(crate) enum Error {
    #[response(status = 500)]
    DatabaseError(String),
    #[response(status = 500)]
    ServerError(String),
    #[response(status = 500)]
    SmtpError(String),
    #[response(status = 500)]
    EnvVarError(String),
    #[response(status = 401)]
    Unauthorized(String),
    #[response(status = 400)]
    ValidationError(String),
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Error::DatabaseError(format!("DatabaseError: {}", value))
    }
}

impl From<rocket::Error> for Error {
    fn from(value: rocket::Error) -> Self {
        Error::ServerError(value.to_string())
    }
}

impl From<lettre::transport::smtp::Error> for Error {
    fn from(value: lettre::transport::smtp::Error) -> Self {
        Error::SmtpError(format!("SMTP Error: {:?}", value))
    }
}

impl From<std::env::VarError> for Error {
    fn from(value: std::env::VarError) -> Self {
        Error::EnvVarError(value.to_string())
    }
}

impl From<dotenv::Error> for Error {
    fn from(value: dotenv::Error) -> Self {
        Error::EnvVarError(value.to_string())
    }
}

impl From<lettre::error::Error> for Error {
    fn from(value: lettre::error::Error) -> Self {
        Error::SmtpError(value.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_value: FromUtf8Error) -> Self {
        Error::ServerError("Failed to parse Text as utf8".to_owned())
    }
}

impl From<tokio::sync::broadcast::error::SendError<ChangeMessage>> for Error {
    fn from(_value: tokio::sync::broadcast::error::SendError<ChangeMessage>) -> Self {
        Error::ServerError("Internal message passing failure".to_owned())
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for Error {
    fn from(_value: tokio::sync::broadcast::error::RecvError) -> Self {
        Error::ServerError("Internal message passing failure".to_owned())
    }
}

impl From<std::io::Error> for Error {
    fn from(_value: std::io::Error) -> Self {
        Error::ServerError("IO Error".to_owned())
    }
}
