use axum::response::sse::Event;
use rusqlite::Row;
use serde::{Deserialize, Serialize};

use crate::{auth::DatabasePermissions, database::Value, error::CRRError};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub(crate) struct Changeset {
    table: String,
    pk: Value,
    cid: Option<String>,
    val: Value,
    col_version: i64,
    db_version: i64,
    #[serde(with = "crate::serde_base64")]
    site_id: Vec<u8>,
}

impl Changeset {
    pub(crate) fn size(&self) -> usize {
        self.table.len()
            + self.pk.size()
            + self.cid.as_ref().map(|cid| cid.len()).unwrap_or_default()
            + self.val.size()
            + 16
            + self.site_id.len()
    }

    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    pub(crate) fn pk(&self) -> &Value {
        &self.pk
    }

    pub(crate) fn cid(&self) -> Option<&str> {
        self.cid.as_ref().map(String::as_str)
    }

    pub(crate) fn val(&self) -> &Value {
        &self.val
    }

    pub(crate) fn col_version(&self) -> i64 {
        self.col_version
    }

    pub(crate) fn db_version(&self) -> i64 {
        self.db_version
    }

    pub(crate) fn site_id(&self) -> &Vec<u8> {
        &self.site_id
    }
}

impl<'a> TryFrom<&Row<'a>> for Changeset {
    type Error = CRRError;

    fn try_from(row: &Row<'a>) -> Result<Self, Self::Error> {
        Ok(Changeset {
            table: row.get(0)?,
            pk: row.get(1)?,
            cid: row.get(2)?,
            val: row.get(3)?,
            col_version: row.get(4)?,
            db_version: row.get(5)?,
            site_id: row.get(6)?,
        })
    }
}

impl TryFrom<Changeset> for Event {
    type Error = CRRError;

    fn try_from(value: Changeset) -> Result<Self, Self::Error> {
        Ok(Event::default().event("change").json_data(value)?)
    }
}
