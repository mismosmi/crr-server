use std::fs;

use axum_extra::extract::CookieJar;
use rusqlite::{named_params, OpenFlags};

use crate::error::CRRError;

use super::DatabasePermissions;

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
                FROM permissions
                WHERE role_id IN (SELECT role_id FROM user_roles WHERE user_id = :user_id)
                AND database_name = :database_name
                AND pfull = TRUE
            ",
        )?;

        let granted = query.exists(named_params! {
            ":user_id": user_id,
            ":database_name": db_name
        })?;

        Ok(granted)
    }

    fn get_permissions_for_user(
        &self,
        user_id: i64,
        database_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        let mut stmt = self.prepare(
            "
                SELECT 
                    table_name,
                    pread,
                    pinsert,
                    pupdate,
                    pdelete,
                    pfull
                FROM permissions
                WHERE role_id IN (SELECT role_id FROM user_roles WHERE user_id = :user_id)
                AND database_name = :database_name
            ",
        )?;

        let mut rows = stmt.query(named_params! {
            ":user_id": user_id,
            ":database_name": database_name
        })?;

        let mut permissions = DatabasePermissions::default();

        while let Ok(Some(row)) = rows.next() {
            let table_name: Option<String> = row.get(0)?;
            let pread: bool = row.get(1)?;
            let pinsert: bool = row.get(2)?;
            let pupdate: bool = row.get(3)?;
            let pdelete: bool = row.get(4)?;
            let pfull: bool = row.get(5)?;

            match table_name {
                Some(table_name) => {
                    if pread {
                        permissions.grant_table_read(table_name);
                    }
                    if pinsert {
                        permissions.grant_table_insert(table_name);
                    }
                    if pupdate {
                        permissions.grant_table_update(table_name);
                    }
                    if pdelete {
                        permissions.grant_table_delete(table_name);
                    }
                    if pfull {
                        permissions.grant_table_full(table_name);
                    }
                }
                None => {
                    if pread {
                        permissions.grant_read();
                    }
                    if pinsert {
                        permissions.grant_insert();
                    }
                    if pupdate {
                        permissions.grant_update();
                    }
                    if pdelete {
                        permissions.grant_delete();
                    }
                    if pfull {
                        permissions.grant_full();
                    }
                }
            }
        }

        Ok(permissions)
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

        let permissions = self.get_permissions_for_user(user_id, db_name)?;

        if permissions.is_empty() {
            return Err(CRRError::unauthorized(format!(
                "User has no access to database {}",
                db_name
            )));
        }

        Ok(permissions)
    }

    fn update_permissions(
        &self,
        role_id: i64,
        database_name: &str,
        permissions: &DatabasePermissions,
    ) -> Result<(), CRRError> {
        let query = "
            INSERT INTO permissions
                (role_id, database_name, table_name, pread, pinsert, pupdate, pdelete, pfull) 
            VALUES 
                (:role_id, :database_name, :table_name, :pread, :pinsert, :pupdate, :pdelete, :pfull) 
            ON CONFLICT (role_id, database_name, table_name)
            DO UPDATE SET
                pread = excluded.pread,
                pinsert = excluded.pinsert,
                pupdate = excluded.pupdate,
                pdelete = excluded.pdelete,
                pfull = excluded.pfull;
        ";

        let mut stmt = self.prepare(query)?;

        permissions.apply(|table_name, permissions| {
            stmt.execute(named_params! {
                ":role_id": role_id,
                ":database_name": database_name,
                ":table_name": table_name,
                ":pread": permissions.read(),
                ":pinsert": permissions.insert(),
                ":pupdate": permissions.update(),
                ":pdelete": permissions.delete(),
                ":pfull": permissions.full()
            })?;

            Ok(())
        })
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

#[cfg(test)]
mod tests {
    #[test]
    fn do_nothing() {}
}
