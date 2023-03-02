#[derive(rocket::Responder, Debug)]
#[response(status = 500)]
pub(crate) enum Error {
    DatabaseError(String),
    ServerError(String),
    SmtpError(String),
    EnvVarError(String),
    Unauthorized(String),
    MigrationError(String),
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
