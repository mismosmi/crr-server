use std::{fs, path::PathBuf};

use axum_extra::extract::CookieJar;
use rusqlite::{named_params, OpenFlags};

use crate::{database::Database, error::CRRError};

use super::{
    permissions::{self, PartialPermissions},
    DatabasePermissions,
};

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
        use crate::tests::TestEnv;

        Self {
            conn: rusqlite::Connection::open(env.folder().join(TestEnv::DB_NAME))
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
        let user_id = self.authenticate_user(cookies)?;

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
        let user_id = self.authenticate_user(cookies)?;

        match self.get_permissions(cookies, db_name) {
            Err(CRRError::Unauthorized(message)) => {
                let file: PathBuf = Database::file_name(&db_name).into();
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

#[cfg(test)]
mod tests {
    #[test]
    fn do_nothing() {}
}
