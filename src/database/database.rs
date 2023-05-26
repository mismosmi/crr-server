use std::path::PathBuf;

use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    named_params, params_from_iter, LoadExtensionGuard, ToSql,
};

use crate::{
    app_state::AppEnv,
    auth::{AllowedTables, DatabasePermissions},
    database::changes::Changeset,
    error::CRRError,
};

const CHANGE_BUFFER_SIZE: usize = 1_000_000;

pub(crate) struct Database {
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

        println!("load extension {}", extension_name);

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

    pub(crate) fn changes<'d, 's>(
        &'d mut self,
        site_id: &'s Vec<u8>,
    ) -> Result<ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd>, CRRError>
    where
        's: 'd,
    {
        let readable_tables = self.permissions.readable_tables();

        if readable_tables.is_empty() {
            return Err(CRRError::Unauthorized(
                "User is not authorized to read database".to_string(),
            ));
        }

        let query = match &readable_tables {
            AllowedTables::All => "
                SELECT \"table\", pk, cid, val, col_version, db_version, site_id
                FROM crsql_changes
                WHERE db_version > ?
                AND site_id IS NOT ?
            "
            .to_string(),
            AllowedTables::Some(table_names) => format!(
                "
                    SELECT \"table\", pk, cid, val, col_version, db_version, site_id
                    FROM crsql_changes
                    WHERE db_version > ?
                    AND site_id IS NOT ?
                    AND \"table\" IN ({})
                ",
                vec!["?"].repeat(table_names.len()).join(", ")
            ),
        };

        Ok(ChangesIter::new(move || {
            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;

                let mut stmt = self.prepare(&query)?;

                let mut params: Vec<Box<dyn ToSql>> = Vec::new();

                params.push(Box::new(self.db_version));
                params.push(Box::new(site_id));

                if let AllowedTables::Some(table_names) = &readable_tables {
                    for table_name in table_names {
                        params.push(Box::new(table_name));
                    }
                }

                let mut rows = stmt.query(params_from_iter(params.iter()))?;

                while let Ok(Some(row)) = rows.next() {
                    let changeset: Changeset = row.try_into()?;

                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            if let Some(changeset) = buffer.last() {
                self.db_version = changeset.db_version();
            }

            Ok((buffer, has_next_page))
        }))
    }

    pub(crate) fn all_changes<'d>(
        &'d mut self,
    ) -> ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd> {
        ChangesIter::new(move || {
            if !self.permissions.full() {
                return Err(CRRError::Unauthorized(
                    "Full access is required to listen to all changes".to_owned(),
                ));
            }

            let query = "
                SELECT \"table\", pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid())
                FROM crsql_changes
                WHERE db_version > ?
            ";

            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;
                let mut stmt = self.conn.prepare(query)?;
                let mut rows = stmt.query([&self.db_version])?;

                while let Some(row) = rows.next()? {
                    let changeset: Changeset = row.try_into()?;
                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            if let Some(changeset) = buffer.last() {
                self.db_version = changeset.db_version();
            }

            Ok((buffer, has_next_page))
        })
    }

    pub(crate) fn apply_changes(&mut self, changes: Vec<Changeset>) -> Result<(), CRRError> {
        let query = "
            INSERT INTO crsql_changes (\"table\", pk, cid, val, col_version, db_version, site_id)
            VALUES (:table, :pk, :cid, :val, :col_version, :db_version, :site_id)
        ";

        let mut stmt = self.prepare(query)?;

        for changeset in changes {
            if changeset.cid().is_none() && !self.permissions.delete_table(changeset.table()) {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to delete from table {}",
                    changeset.table()
                )));
            } else if changeset.col_version() == 1
                && !self.permissions.insert_table(changeset.table())
            {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to insert into table {}",
                    changeset.table()
                )));
            } else if !self.permissions.update_table(changeset.table()) {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to update table {}",
                    changeset.table()
                )));
            }

            stmt.insert(named_params! {
                ":table": changeset.table(),
                ":pk": changeset.pk(),
                ":cid": changeset.cid(),
                ":val": changeset.val(),
                ":col_version": changeset.col_version(),
                ":db_version": changeset.db_version(),
                ":site_id": changeset.site_id(),
            })?;
        }

        Ok(())
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

pub(crate) struct ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    load_page: std::sync::Mutex<F>,
    current_page: <Vec<Changeset> as IntoIterator>::IntoIter,
    has_next_page: bool,
}

impl<F> ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    fn new(load_page: F) -> Self {
        Self {
            load_page: std::sync::Mutex::new(load_page),
            current_page: Vec::new().into_iter(),
            has_next_page: true,
        }
    }
}

impl<F> Iterator for ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    type Item = Result<Changeset, CRRError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(changeset) = self.current_page.next() {
            return Some(Ok(changeset));
        }

        if self.has_next_page {
            match self
                .load_page
                .lock()
                .map_err(|_| CRRError::PoisonedLockError("ChangesIter::next"))
                .and_then(|mut lock| lock())
            {
                Ok((page, has_next_page)) => {
                    self.current_page = page.into_iter();
                    self.has_next_page = has_next_page;
                    return self.current_page.next().map(|changeset| Ok(changeset));
                }
                Err(error) => return Some(Err(error)),
            }
        }

        None
    }
}
