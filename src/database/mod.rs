pub(crate) mod changes;
pub(crate) mod migrations;

use crate::error::CRRError;
use rocket::serde::{Deserialize, Serialize};
use rusqlite::{
    types::{FromSql, ToSqlOutput, ValueRef},
    LoadExtensionGuard, Row, ToSql,
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

    fn load_crsqlite(conn: &rusqlite::Connection) -> Result<(), CRRError> {
        let os = match std::env::consts::OS {
            "macos" => "darwin",
            "windows" => "windows",
            "linux" => "linux",
            os => return Err(CRRError::ServerError(format!("Unsupported OS: {}", os))),
        };

        let arch = std::env::consts::ARCH;
        let ext = std::env::consts::DLL_EXTENSION;
        let extension_name = format!(
            "./extensions/crsqlite-{os}-{arch}.{ext}",
            os = os,
            arch = arch,
            ext = ext
        );

        println!("load extension {}", extension_name);

        unsafe {
            let _guard = LoadExtensionGuard::new(conn)?;
            conn.load_extension(extension_name, Some("sqlite3_crsqlite_init"))?;
        }

        Ok(())
    }

    pub(crate) fn open(name: String) -> Result<Self, CRRError> {
        let conn = rusqlite::Connection::open(Self::file_name(&name))?;

        Self::load_crsqlite(&conn)?;

        Ok(Self {
            conn,
            name,
            db_version: 0,
        })
    }

    pub(crate) fn open_readonly(name: String, db_version: i64) -> Result<Self, CRRError> {
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

    #[cfg(test)]
    pub(crate) fn open_for_test(env: &crate::tests::TestEnv) -> Self {
        let conn = rusqlite::Connection::open(env.folder().join("data.sqlite3"))
            .expect("Failed to open test database");

        Self::load_crsqlite(&conn).expect("Failed to load crsqlite");

        Self {
            conn,
            name: "data".to_owned(),
            db_version: 0,
        }
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

#[derive(Clone, Serialize, Debug, PartialEq, Deserialize)]
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

    #[cfg(test)]
    pub(crate) fn text(value: &str) -> Self {
        Self::Text(format!("'{}'", value))
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use rusqlite::types::Value as RusqliteValue;

        let value: RusqliteValue = value.into();
        match value {
            RusqliteValue::Blob(value) => Ok(Self::Blob(value)),
            RusqliteValue::Integer(value) => Ok(Self::Integer(value)),
            RusqliteValue::Real(value) => Ok(Self::Real(value)),
            RusqliteValue::Null => Ok(Self::Null),
            RusqliteValue::Text(value) => Ok(Self::Text(value)),
        }
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Self::Blob(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Blob(&value))),
            Self::Integer(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Integer(value.clone()))),
            Self::Real(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Real(value.clone()))),
            Self::Null => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
            Self::Text(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes()))),
        }
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
#[serde(crate = "rocket::serde")]
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

pub(crate) type ChangeMessage = Result<Changeset, CRRError>;
