use axum::response::sse::Event;
use serde::{Deserialize, Serialize};

use crate::error::{CRRError, HttpError};

use super::Changeset;

#[derive(Clone, Serialize, Debug, Deserialize)]
pub(crate) struct Migration {
    version: i64,
    sql: String,
}

impl Migration {
    pub(crate) fn new(version: i64, sql: String) -> Self {
        Self { version, sql }
    }
    pub(crate) fn version(&self) -> i64 {
        self.version
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Message {
    Change(Changeset),
    Migration(Migration),
    Error(HttpError),
}

#[cfg(test)]
impl Message {
    pub(crate) fn changeset(self) -> Option<Changeset> {
        match self {
            Self::Change(changeset) => Some(changeset),
            _ => None,
        }
    }
}

impl TryFrom<Migration> for Event {
    type Error = CRRError;

    fn try_from(value: Migration) -> Result<Self, Self::Error> {
        Ok(Event::default().event("migration").json_data(value)?)
    }
}

impl From<Result<Changeset, CRRError>> for Message {
    fn from(value: Result<Changeset, CRRError>) -> Self {
        match value {
            Ok(changeset) => Self::Change(changeset),
            Err(error) => Self::Error(error.into()),
        }
    }
}

impl From<Result<Migration, CRRError>> for Message {
    fn from(value: Result<Migration, CRRError>) -> Self {
        match value {
            Ok(migration) => Self::Migration(migration),
            Err(error) => Self::Error(error.into()),
        }
    }
}
