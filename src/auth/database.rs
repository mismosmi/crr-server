use std::{fs, path::PathBuf, sync::Arc};

use axum::{async_trait, extract::FromRequestParts, http::request::Parts};
use axum_extra::extract::CookieJar;
use rusqlite::{named_params, OpenFlags};

use crate::{
    app_state::{AppEnv, AppState},
    database::Database,
    error::CRRError,
};

use super::{permissions::PartialPermissions, DatabasePermissions, COOKIE_NAME};

pub struct AuthDatabase {
    conn: rusqlite::Connection,
    env: Arc<AppEnv>,
}

impl AuthDatabase {
    const RESERVED_NAMES: [&str; 2] = ["auth", "sync"];

    fn file_path(env: &AppEnv) -> PathBuf {
        let mut path = PathBuf::from(env.data_dir());
        path.push("auth.sqlite3");
        path
    }

    pub fn open(env: Arc<AppEnv>) -> Result<Self, CRRError> {
        Ok(Self {
            conn: rusqlite::Connection::open(Self::file_path(&env))?,
            env,
        })
    }

    pub(crate) fn open_readonly(env: Arc<AppEnv>) -> Result<Self, CRRError> {
        Ok(Self {
            conn: rusqlite::Connection::open_with_flags(
                Self::file_path(&env),
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )?,
            env,
        })
    }

    pub fn apply_migrations(&self) -> Result<(), CRRError> {
        tracing::info!("Applying metadata migrations");
        self.execute_batch(&fs::read_to_string("./auth_migrations.sql")?)?;

        Ok(())
    }

    fn authenticate_user(&self, token: &str) -> Result<i64, CRRError> {
        let id: i64 = self
            .prepare("SELECT user_id FROM tokens WHERE token = :token AND expires > 'now'")?
            .query_row(
                named_params! {
                    ":token": token
                },
                |row| row.get(0),
            )?;

        Ok(id)
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
            let read: bool = row.get(1)?;
            let insert: bool = row.get(2)?;
            let update: bool = row.get(3)?;
            let delete: bool = row.get(4)?;
            let full: bool = row.get(5)?;

            match table_name {
                Some(table_name) => {
                    if full {
                        permissions.set_table_full(table_name);
                    } else {
                        permissions.set_table(
                            table_name,
                            PartialPermissions {
                                read,
                                insert,
                                update,
                                delete,
                            },
                        )
                    }
                }
                None => {
                    if full {
                        permissions.set_full();
                        return Ok(permissions);
                    } else {
                        permissions.set(PartialPermissions {
                            read,
                            insert,
                            update,
                            delete,
                        })
                    }
                }
            }
        }

        Ok(permissions)
    }

    pub(crate) fn get_permissions(
        &self,
        cookies: &CookieJar,
        db_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        if Self::RESERVED_NAMES.contains(&db_name) {
            return Err(CRRError::ReservedName(db_name.to_owned()));
        }

        let token = cookies
            .get(COOKIE_NAME)
            .ok_or(CRRError::Unauthorized("No Token found".to_string()))?
            .value();

        if Some(token) == self.env.admin_token().as_deref() {
            return Ok(DatabasePermissions::Full);
        }

        let user_id = self.authenticate_user(token)?;

        let permissions = self.get_permissions_for_user(user_id, db_name)?;

        if permissions.is_empty() {
            return Err(CRRError::unauthorized(format!(
                "User has no access to database {}",
                db_name
            )));
        }

        Ok(permissions)
    }

    pub(crate) fn authorize_migration(
        &self,
        cookies: &CookieJar,
        db_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        let token = cookies
            .get(COOKIE_NAME)
            .ok_or(CRRError::Unauthorized("No Token found".to_string()))?
            .value();

        if Some(token) == self.env.admin_token().as_deref() {
            return Ok(DatabasePermissions::Full);
        }

        let user_id = self.authenticate_user(token)?;

        match self.get_permissions(cookies, db_name) {
            Err(CRRError::Unauthorized(message)) => {
                let file: PathBuf = Database::file_path(&self.env, &db_name).into();
                if file.exists() {
                    Err(CRRError::Unauthorized(message))
                } else {
                    let permissions = DatabasePermissions::Full;

                    self.conn.execute("BEGIN", [])?;

                    let role_id = self
                        .conn
                        .prepare("INSERT INTO roles () VALUES ()")?
                        .insert([])?;

                    self.conn
                        .prepare(
                            "INSERT INTO user_roles (user_id, role_id) VALUES (:user_id, :role_id)",
                        )?
                        .insert(named_params! {":user_id": user_id, ":role_id": role_id})?;

                    self.update_permissions(role_id, db_name, &permissions)?;

                    self.conn.execute("COMMIT", [])?;

                    Ok(permissions)
                }
            }
            result => result,
        }
    }

    pub(crate) fn update_permissions(
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

#[async_trait]
impl FromRequestParts<AppState> for AuthDatabase {
    type Rejection = CRRError;

    async fn from_request_parts(
        _parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        AuthDatabase::open(state.env().clone())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn do_nothing() {}
}
