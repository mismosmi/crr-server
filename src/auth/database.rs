use std::{fs, path::PathBuf, sync::Arc};

use rusqlite::named_params;

use crate::{app_state::AppEnv, error::CRRError};

use super::{permissions::PartialPermissions, DatabasePermissions};

pub struct AuthDatabase {
    conn: rusqlite::Connection,
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
        })
    }

    pub fn apply_migrations(&self) -> Result<(), CRRError> {
        tracing::info!("Applying metadata migrations");
        self.execute_batch(&fs::read_to_string("./auth_migrations.sql")?)?;

        Ok(())
    }

    fn authenticate_user(&self, token: &str) -> Result<i64, CRRError> {
        let id: i64 = self
            .prepare("SELECT user_id FROM tokens WHERE token = :token AND expires < 'now'")?
            .query_row(
                named_params! {
                    ":token": token
                },
                |row| row.get(0),
            )
            .map_err(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => {
                    CRRError::Unauthorized("Invalid Token".to_owned())
                }
                error => error.into(),
            })?;

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
        token: &str,
        db_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        if Self::RESERVED_NAMES.contains(&db_name) {
            return Err(CRRError::ReservedName(db_name.to_owned()));
        }

        let user_id = self.authenticate_user(token)?;

        let permissions = self.get_permissions_for_user(user_id, db_name)?;

        if permissions.is_empty() {
            if self.database_exists(db_name)? {
                return Err(CRRError::unauthorized(format!(
                    "User has no access to database {}",
                    db_name
                )));
            } else {
                return self.create_owning_role(user_id, db_name);
            }
        }

        Ok(permissions)
    }

    //pub(crate) fn update_permissions(
    //    &self,
    //    role_id: i64,
    //    database_name: &str,
    //    permissions: &DatabasePermissions,
    //) -> Result<(), CRRError> {
    //    let query = "
    //        INSERT INTO permissions
    //            (role_id, database_name, table_name, pread, pinsert, pupdate, pdelete, pfull)
    //        VALUES
    //            (:role_id, :database_name, :table_name, :pread, :pinsert, :pupdate, :pdelete, :pfull)
    //        ON CONFLICT (role_id, database_name, table_name)
    //        DO UPDATE SET
    //            pread = excluded.pread,
    //            pinsert = excluded.pinsert,
    //            pupdate = excluded.pupdate,
    //            pdelete = excluded.pdelete,
    //            pfull = excluded.pfull;
    //    ";

    //    let mut stmt = self.prepare(query)?;

    //    permissions.apply(|table_name, permissions| {
    //        stmt.execute(named_params! {
    //            ":role_id": role_id,
    //            ":database_name": database_name,
    //            ":table_name": table_name,
    //            ":pread": permissions.read(),
    //            ":pinsert": permissions.insert(),
    //            ":pupdate": permissions.update(),
    //            ":pdelete": permissions.delete(),
    //            ":pfull": permissions.full()
    //        })?;

    //        Ok(())
    //    })
    //}

    fn database_exists(&self, db_name: &str) -> Result<bool, CRRError> {
        let mut stmt =
            self.prepare("SELECT role_id FROM permissions WHERE database_name = :database_name")?;

        Ok(stmt.exists(named_params! { ":database_name": db_name })?)
    }

    fn create_owning_role(
        &self,
        user_id: i64,
        db_name: &str,
    ) -> Result<DatabasePermissions, CRRError> {
        self.execute("BEGIN", [])?;

        {
            let mut stmt = self.prepare("INSERT INTO roles (name) VALUES (:role_name)")?;
            stmt.insert(named_params! { ":role_name": format!("{}_owners", db_name) })?;
        }

        let role_id = self.last_insert_rowid();

        {
            let mut stmt = self
                .prepare("INSERT INTO user_roles (user_id, role_id) VALUES (:user_id, :role_id)")?;
            stmt.insert(named_params! {
                ":user_id": user_id,
                ":role_id": role_id,
            })?;
        }

        {
            let mut stmt = self.prepare("INSERT INTO permissions (role_id, database_name, pfull) VALUES (:role_id, :db_name, TRUE)")?;
            stmt.insert(named_params! {
                ":role_id": role_id,
                ":db_name": db_name
            })?;
        }

        self.execute("COMMIT", [])?;

        Ok(DatabasePermissions::Create)
    }

    pub(crate) fn get_token_id(&self, token: &str) -> Result<i64, CRRError> {
        Ok(
            self.query_row("SELECT id FROM tokens WHERE token = ?", [token], |row| {
                row.get("id")
            })?,
        )
    }

    pub(crate) fn get_token_by_id(&self, token_id: i64) -> Result<String, CRRError> {
        Ok(
            self.query_row("SELECT token FROM tokens WHERE id = ?", [token_id], |row| {
                row.get("token")
            })?,
        )
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
