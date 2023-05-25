use axum::response::sse::Event;
use serde_json::json;

use crate::error::CRRError;

use super::Changeset;

#[derive(Clone, Debug)]
pub(crate) enum Message {
    Changeset(Changeset),
    Migration,
    DatabaseError(String),
    Error,
}

impl From<Result<Changeset, CRRError>> for Message {
    fn from(value: Result<Changeset, CRRError>) -> Self {
        match value {
            Ok(changeset) => Message::Changeset(changeset),
            Err(CRRError::DatabaseError(error)) => Message::DatabaseError(error.to_string()),
            Err(_) => Message::Error,
        }
    }
}

impl From<Message> for Event {
    fn from(value: Message) -> Self {
        match value {
            Message::Changeset(changeset) => Event::default()
                .event("change")
                .json_data(changeset)
                .unwrap_or(
                    Event::default()
                        .event("error")
                        .json_data(json!({
                            "error": "SerializationError",
                            "message": "Failed to serialize change event"
                        }))
                        .expect("Failed to serialize Error"),
                ),
            Message::Migration => Event::default().event("migration"),
            Message::DatabaseError(message) => Event::default()
                .event("error")
                .json_data(json!({
                    "error": "DatabaseError",
                    "message": message
                }))
                .expect("Failed to serialize Database Error"),
            Message::Error => Event::default()
                .event("error")
                .json_data(json!({
                    "error": "UnexpectedError",
                    "message": "Unexpected Error"
                }))
                .expect("Failed to serialize Error"),
        }
    }
}
