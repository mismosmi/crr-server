use rusqlite::Row;
use serde::{Deserialize, Serialize};

use crate::error::CRRError;

use super::Value;

#[derive(Clone, Deserialize, Serialize, Debug)]
pub(crate) struct Changeset {
    table: String,
    pk: Value,
    cid: String,
    val: Value,
    col_version: i64,
    db_version: i64,
    #[serde(with = "crate::serde_base64")]
    site_id: Vec<u8>,
}

impl Changeset {
    fn size(&self) -> usize {
        self.table.len()
            + self.pk.size()
            + self.cid.len()
            + self.val.size()
            + 16
            + self.site_id.len()
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
