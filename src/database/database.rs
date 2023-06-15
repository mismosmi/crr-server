use std::path::PathBuf;

use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    LoadExtensionGuard,
};

use crate::{app_state::AppEnv, auth::DatabasePermissions, error::CRRError};

pub struct Database {
    conn: rusqlite::Connection,
    name: String,
    db_version: i64,
    permissions: DatabasePermissions,
}

impl Database {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn file_path(env: &AppEnv, name: &str) -> PathBuf {
        let mut path = PathBuf::from(env.data_dir());
        path.push(format!("{}.sqlite3", name));
        path
    }

    pub(crate) fn permissions(&self) -> &DatabasePermissions {
        return &self.permissions;
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

        tracing::info!("load extension {}", extension_name);

        unsafe {
            let _guard = LoadExtensionGuard::new(conn)?;
            conn.load_extension(extension_name, Some("sqlite3_crsqlite_init"))?;
        }

        Ok(())
    }

    fn set_authorizer(conn: &rusqlite::Connection, permissions: DatabasePermissions) {
        fn auth(value: bool) -> Authorization {
            if value {
                Authorization::Allow
            } else {
                Authorization::Deny
            }
        }

        conn.authorizer(if permissions.full() {
            None
        } else {
            Some(move |context: AuthContext| match context.action {
                AuthAction::Select => Authorization::Allow,
                AuthAction::Read { table_name, .. } => auth(permissions.read_table(table_name)),
                AuthAction::Update { table_name, .. } => auth(permissions.update_table(table_name)),
                AuthAction::Insert { table_name } => auth(permissions.insert_table(table_name)),
                AuthAction::Delete { table_name } => auth(permissions.delete_table(table_name)),
                AuthAction::Transaction { operation: _ } => Authorization::Allow,
                _ => Authorization::Deny,
            })
        });
    }

    pub(crate) fn open(
        env: &AppEnv,
        name: String,
        permissions: DatabasePermissions,
    ) -> Result<Self, CRRError> {
        let conn = rusqlite::Connection::open(Self::file_path(env, &name))?;

        Self::load_crsqlite(&conn)?;
        Self::set_authorizer(&conn, permissions.clone());

        Ok(Self {
            conn,
            name,
            db_version: 0,
            permissions,
        })
    }

    pub(crate) fn open_readonly(
        env: &AppEnv,
        name: String,
        db_version: i64,
        permissions: DatabasePermissions,
    ) -> Result<Self, CRRError> {
        let conn = rusqlite::Connection::open_with_flags(
            Self::file_path(env, &name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        Self::load_crsqlite(&conn)?;
        Self::set_authorizer(&conn, permissions.clone());

        Ok(Self {
            conn,
            name,
            db_version,
            permissions,
        })
    }

    pub(crate) fn open_readonly_latest(
        env: &AppEnv,
        name: String,
        permissions: DatabasePermissions,
    ) -> Result<Self, CRRError> {
        let conn = rusqlite::Connection::open_with_flags(
            Self::file_path(env, &name),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        Self::load_crsqlite(&conn)?;
        Self::set_authorizer(&conn, permissions.clone());

        let db_version: i64 = conn.query_row("SELECT crsql_dbversion()", [], |row| row.get(0))?;

        Ok(Self {
            conn,
            name,
            permissions,
            db_version,
        })
    }

    pub(crate) fn db_version(&self) -> i64 {
        self.db_version
    }

    pub(crate) fn set_db_version(&mut self, db_version: i64) {
        self.db_version = db_version;
    }

    pub(crate) fn disable_authorization<'d>(&'d mut self) -> AuthorizedDatabaseHandle<'d> {
        AuthorizedDatabaseHandle::new(self)
    }
}

impl std::ops::Deref for Database {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for Database {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl std::ops::Drop for Database {
    fn drop(&mut self) {
        self.authorizer(None::<for<'r> fn(AuthContext<'r>) -> _>);
        let _err = self.execute("SELECT crsql_finalize()", []);
    }
}

pub(crate) struct AuthorizedDatabaseHandle<'d>(&'d mut Database);

impl<'d> AuthorizedDatabaseHandle<'d> {
    fn new(db: &'d mut Database) -> Self {
        db.authorizer(None::<for<'r> fn(AuthContext<'r>) -> Authorization>);
        Self(db)
    }
}

impl<'d> std::ops::Deref for AuthorizedDatabaseHandle<'d> {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'d> std::ops::DerefMut for AuthorizedDatabaseHandle<'d> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'d> std::ops::Drop for AuthorizedDatabaseHandle<'d> {
    fn drop(&mut self) {
        Database::set_authorizer(&self.0.conn, self.0.permissions.clone())
    }
}
