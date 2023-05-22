use rusqlite::LoadExtensionGuard;

use crate::error::CRRError;

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
        format!("./data/{}.sqlite3", name)
    }

    fn load_crsqlite(conn: &rusqlite::Connection) -> Result<(), CRRError> {
        let os = match std::env::consts::OS {
            "macos" => "darwin",
            "windows" => "windows",
            "linux" => "linux",
            os => return Err(CRRError::UnsupportedOS(os.to_owned())),
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
