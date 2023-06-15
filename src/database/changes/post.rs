use axum::extract::{Json, Path, State};
use rusqlite::named_params;

use crate::{app_state::AppState, auth::DatabasePermissions, database::Database, error::CRRError};

use super::Changeset;

pub(crate) async fn post_changes(
    Path(db_name): Path<String>,
    State(state): State<AppState>,
    permissions: DatabasePermissions,
    Json(changes): Json<Vec<Changeset>>,
) -> Result<(), CRRError> {
    let mut db = Database::open(&state.env(), db_name, permissions)?;

    db.apply_changes(changes)?;

    Ok(())
}

impl Database {
    pub(crate) fn apply_changes(&mut self, changes: Vec<Changeset>) -> Result<(), CRRError> {
        let query = "
            INSERT INTO crsql_changes (\"table\", pk, cid, val, col_version, db_version, site_id)
            VALUES (:table, :pk, :cid, :val, :col_version, :db_version, :site_id)
        ";

        let authorized = self.disable_authorization();

        let mut stmt = authorized.prepare(query)?;

        for changeset in changes {
            if changeset.cid() == Some("__crsql_del") {
                if !authorized.permissions().delete_table(changeset.table()) {
                    return Err(CRRError::Unauthorized(format!(
                        "User is not authorized to delete from table \"{}\"",
                        changeset.table()
                    )));
                }
            } else if changeset.col_version() == 1 {
                if !authorized.permissions().insert_table(changeset.table()) {
                    return Err(CRRError::Unauthorized(format!(
                        "User is not authorized to insert into table \"{}\"",
                        changeset.table()
                    )));
                }
            } else {
                if !authorized.permissions().update_table(changeset.table()) {
                    return Err(CRRError::Unauthorized(format!(
                        "User is not authorized to update table \"{}\"",
                        changeset.table()
                    )));
                }
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::extract::{Path, State};

    use super::post_changes;
    use crate::{
        app_state::{AppEnv, AppState},
        auth::{DatabasePermissions, PartialPermissions},
        database::{changes::Changeset, migrate::tests::setup_foo},
        error::CRRError,
    };

    fn get_changes() -> [Vec<Changeset>; 3] {
        let env = AppEnv::test_env();

        setup_foo(&env);

        let mut db = env.test_db();

        {
            let mut stmt = db.prepare("INSERT INTO foo (bar) VALUES (?)").unwrap();
            stmt.insert(["a"]).unwrap();
            stmt.insert(["b"]).unwrap();
            stmt.insert(["c"]).unwrap();
        }

        let inserts = db
            .all_changes()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .unwrap();

        {
            let mut stmt = db.prepare("UPDATE foo SET bar=?").unwrap();
            stmt.execute(["d"]).unwrap();
        }

        let updates = db
            .all_changes()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .unwrap();

        {
            let mut stmt = db.prepare("DELETE FROM foo WHERE id = ?").unwrap();
            stmt.execute([1]).unwrap();
        }

        let deletes = db
            .all_changes()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .unwrap();

        [inserts, updates, deletes]
    }

    #[tokio::test]
    async fn post_changes_with_read_permissions() {
        let state = AppState::test_state();

        setup_foo(state.env());

        let [inserts, updates, deletes] = get_changes();

        let permissions = DatabasePermissions::Partial {
            database: PartialPermissions {
                read: true,
                insert: false,
                update: false,
                delete: false,
            },
            tables: HashMap::new(),
        };

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(inserts.clone()),
        )
        .await
        .is_err());

        state.env().test_db().apply_changes(inserts).unwrap();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(updates),
        )
        .await
        .is_err());

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions,
            axum::extract::Json(deletes),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn post_changes_with_insert_permissions() {
        let state = AppState::test_state();
        setup_foo(state.env());

        let permissions = DatabasePermissions::Partial {
            database: PartialPermissions {
                read: false,
                insert: true,
                update: false,
                delete: false,
            },
            tables: HashMap::new(),
        };
        let [inserts, updates, deletes] = get_changes();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(inserts),
        )
        .await
        .is_ok());

        assert_eq!(
            state
                .env()
                .test_db()
                .prepare("SELECT bar FROM foo")
                .unwrap()
                .query_map([], |row| { row.get::<usize, String>(0) })
                .unwrap()
                .collect::<Result<Vec<String>, rusqlite::Error>>()
                .unwrap(),
            vec!["a", "b", "c"]
        );

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(updates),
        )
        .await
        .is_err());

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions,
            axum::extract::Json(deletes),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn post_changes_with_update_permissions() {
        let state = AppState::test_state();
        setup_foo(state.env());

        let permissions = DatabasePermissions::Partial {
            database: PartialPermissions {
                read: false,
                insert: false,
                update: true,
                delete: false,
            },
            tables: HashMap::new(),
        };
        let [inserts, updates, deletes] = get_changes();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(inserts.clone())
        )
        .await
        .is_err());

        state.env().test_db().apply_changes(inserts).unwrap();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(updates)
        )
        .await
        .is_ok());

        assert_eq!(
            state
                .env()
                .test_db()
                .prepare("SELECT bar FROM foo")
                .unwrap()
                .query_map([], |row| { row.get::<usize, String>(0) })
                .unwrap()
                .collect::<Result<Vec<String>, rusqlite::Error>>()
                .unwrap(),
            vec!["d", "d", "d"]
        );

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions,
            axum::extract::Json(deletes),
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn post_changes_with_delete_permissions() {
        let state = AppState::test_state();
        setup_foo(state.env());

        let permissions = DatabasePermissions::Partial {
            database: PartialPermissions {
                read: false,
                insert: false,
                update: false,
                delete: true,
            },
            tables: HashMap::new(),
        };
        let [inserts, updates, deletes] = get_changes();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(inserts.clone())
        )
        .await
        .is_err());

        state.env().test_db().apply_changes(inserts).unwrap();

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions.clone(),
            axum::extract::Json(updates)
        )
        .await
        .is_err());

        assert!(post_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            State(state.clone()),
            permissions,
            axum::extract::Json(deletes),
        )
        .await
        .is_ok());

        assert_eq!(
            state
                .env()
                .test_db()
                .prepare("SELECT bar FROM foo")
                .unwrap()
                .query_map([], |row| { row.get::<usize, String>(0) })
                .unwrap()
                .collect::<Result<Vec<String>, rusqlite::Error>>()
                .unwrap(),
            vec!["b", "c"]
        );
    }
}
