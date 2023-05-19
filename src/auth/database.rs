use std::{collections::HashMap, fs};

use axum_extra::extract::CookieJar;
use rusqlite::{named_params, OpenFlags};

use crate::error::CRRError;

use super::{permissions::TablePermissions, DatabasePermissions};

pub(crate) struct AuthDatabase {
    conn: rusqlite::Connection,
}

impl AuthDatabase {
    pub(crate) fn open() -> Result<Self, CRRError> {
        Ok(Self {
            conn: rusqlite::Connection::open("./data/auth.sqlite3")?,
        })
    }

    pub(crate) fn open_readonly() -> Result<Self, CRRError> {
        Ok(Self {
            conn: rusqlite::Connection::open_with_flags(
                "./data/auth.sqlite3",
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_for_test(env: &crate::tests::TestEnv) -> Self {
        Self {
            conn: rusqlite::Connection::open(env.folder().join("auth.sqlite3"))
                .expect("failed to open metadata database"),
        }
    }

    pub(crate) fn apply_migrations(&self) -> Result<(), CRRError> {
        tracing::info!("Applying metadata migrations");
        self.execute_batch(&fs::read_to_string("./auth_migrations.sql")?)?;

        Ok(())
    }

    fn authenticate_user(&self, cookies: &CookieJar) -> Result<i64, CRRError> {
        let id: i64 = self.prepare("SELECT user_id FROM tokens WHERE token = :token AND expires > 'now'")?
            .query_row(named_params! {
                ":token": cookies.get(super::COOKIE_NAME).ok_or(CRRError::Unauthorized("No Token Found".to_string()))?.value()
            }, |row| row.get(0))?;

        Ok(id)
    }

    fn check_ownership(&self, user_id: i64, db_name: &str) -> Result<bool, CRRError> {
        let mut query = self.prepare(
            "
            SELECT TRUE 
            FROM database_owners
            WHERE user_id = :user_id
            AND database_name = :database_name
            ",
        )?;

        let granted = query.exists(named_params! {
            ":user_id": self.id,
            ":database_name": db_name
        })?;

        Ok(granted)
    }

    fn get_table_permissions(
        &self,
        user_id: i64,
        database_name: &str,
    ) -> Result<HashMap<String, TablePermissions>, CRRError> {
        let mut stmt = self.prepare(
            "
                SELECT 
                    table_permissions.table_name,
                    table_permissions.pread,
                    table_permissions.pinsert,
                    table_permissions.pupdate,
                    table_permissions.pdelete
                FROM user_roles
                LEFT JOIN table_permissions
                ON user_roles.role_id = table_permissions.role_id
                WHERE table_permissions.database_name = :database_name
                AND user_roles.user_id = :user_id
            ",
        )?;

        let mut rows = stmt.query(named_params! {
            ":user_id": self.id,
            ":database_name": database_name
        })?;

        let mut table_permissions: HashMap<String, TablePermissions> = HashMap::new();

        while let Ok(Some(row)) = rows.next() {
            let table_name = row.get(0)?;
            let pread = row.get(1)?;
            let pinsert = row.get(2)?;
            let pupdate = row.get(3)?;
            let pdelete = row.get(4)?;

            let permissions = table_permissions
                .entry(table_name)
                .or_insert(TablePermissions::default());

            permissions.merge_read(pread);
            permissions.merge_insert(pinsert);
            permissions.merge_update(pupdate);
            permissions.merge_delete(pdelete);
        }

        Ok(table_permissions)
    }

    pub(crate) fn authorize_owned_access(
        &self,
        cookies: &CookieJar,
        db_name: &str,
    ) -> Result<(), CRRError> {
        let user_id = self.authenticate_user(cookies)?;

        if self.check_ownership(user_id, db_name)? {
            return Ok(());
        }

        return Err(CRRError::unauthorized(format!(
            "User does not own database \"{}\"",
            db_name
        )));
    }

    pub(crate) fn get_permissions(
        &self,
        cookies: &CookieJar,
        db_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        let user_id = self.authenticate_user(cookies)?;

        if self.check_ownership(user_id, db_name)? {
            return Ok(DatabasePermissions::Full);
        }

        let table_permissions = self.get_table_permissions(user_id, db_name)?;

        if table_permissions.is_empty() {
            return Err(CRRError::unauthorized(format!(
                "User has no access to database {}",
                db_name
            )));
        }

        Ok(DatabasePermissions::Partial(table_permissions))
    }
}

impl std::ops::Deref for AuthDatabase {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl std::ops::DerefMut for AuthDatabase {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}
