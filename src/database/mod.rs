pub(crate) mod changes;
pub(crate) mod migrations;

use crate::error::Error;
use rocket::serde::Serialize;
use rusqlite::{
    types::{FromSql, ValueRef},
    LoadExtensionGuard, Row,
};

pub(crate) struct Database {
    conn: rusqlite::Connection,
    name: String,
    db_version: i64,
}

impl Database {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn file_name(name: &str) -> String {
        format!("./data/databases/{}.sqlite3", name)
    }

    fn load_crsqlite(conn: &rusqlite::Connection) -> Result<(), Error> {
        let os = match std::env::consts::OS {
            "macos" => "darwin",
            "windows" => "windows",
            "linux" => "linux",
            os => return Err(Error::ServerError(format!("Unsupported OS: {}", os))),
        };

        let arch = std::env::consts::ARCH;
        let ext = std::env::consts::DLL_EXTENSION;
        let extension_name = format!(
            "./extensions/crsqlite-{os}-{arch}.{ext}",
            os = os,
            arch = arch,
            ext = ext
        );

        unsafe {
            let _guard = LoadExtensionGuard::new(conn)?;
            conn.load_extension(extension_name, None)?;
        }

        Ok(())
    }

    pub(crate) fn open(name: String) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open(Self::file_name(&name))?;

        Self::load_crsqlite(&conn)?;

        Ok(Self {
            conn,
            name,
            db_version: 0,
        })
    }

    pub(crate) fn open_readonly(name: String, db_version: i64) -> Result<Self, Error> {
        let conn = rusqlite::Connection::open_with_flags(
            Self::file_name(&name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        Self::load_crsqlite(&conn)?;

        Ok(Self {
            conn,
            name,
            db_version,
        })
    }
}

impl std::ops::Deref for Database {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::Drop for Database {
    fn drop(&mut self) {
        let _err = self.execute("SELECT crsql_finalize()", []);
    }
}

#[derive(Clone, Serialize)]
#[serde(crate = "rocket::serde")]
enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    fn size(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Integer(_) => 8,
            Self::Real(_) => 8,
            Self::Text(value) => value.len(),
            Self::Blob(value) => value.len(),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            ValueRef::Null => Ok(Self::Null),
            ValueRef::Integer(value) => Ok(Self::Integer(value)),
            ValueRef::Real(value) => Ok(Self::Real(value)),
            ValueRef::Text(value) => Ok(Self::Text(
                String::from_utf8(Vec::from(value))
                    .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))?,
            )),
            ValueRef::Blob(value) => Ok(Self::Blob(Vec::from(value))),
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(crate = "rocket::serde")]
pub(crate) struct Changeset {
    table: String,
    pk: Value,
    cid: String,
    val: Value,
    col_version: i64,
    db_version: i64,
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

    fn db_version(&self) -> i64 {
        self.db_version
    }
}

impl<'a> TryFrom<&Row<'a>> for Changeset {
    type Error = Error;

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

pub(crate) type ChangeMessage = Result<Changeset, Error>;
