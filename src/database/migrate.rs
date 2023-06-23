use crate::{auth::DatabasePermissions, error::CRRError, AppState};
use axum::extract::{Json, Path, State};
use lazy_static::lazy_static;
use regex::Regex;
use rusqlite::Connection;
use serde::Deserialize;

use super::Database;

#[derive(Deserialize)]
pub(crate) struct MigratePostData {
    queries: Vec<String>,
}

pub(crate) async fn post_migrate(
    Path(db_name): Path<String>,
    permissions: DatabasePermissions,
    State(state): State<AppState>,
    Json(data): Json<MigratePostData>,
) -> Result<(), CRRError> {
    state.change_manager().kill_connection(&db_name).await;

    let mut db = Database::open(&state.env(), db_name, permissions)?;

    db.apply_migrations(&data.queries)?;

    Ok(())
}

impl Database {
    pub(crate) fn apply_migrations(&mut self, migrations: &Vec<String>) -> Result<(), CRRError> {
        if !self.permissions().full() {
            return Err(CRRError::Unauthorized(
                "User must be authorized with full access to the database to apply migrations"
                    .to_owned(),
            ));
        }

        let savepoint = self.savepoint()?;

        for migration in migrations {
            Self::apply_migration(&savepoint, migration)?;
        }

        savepoint.commit()?;

        Ok(())
    }

    fn apply_migration(conn: &Connection, sql: &str) -> Result<(), CRRError> {
        lazy_static! {
            static ref RE_CREATE: Regex = Regex::new(r"/CREATE TABLE\w(.+)\w\(/i")
                .expect("Failed to compile create table regex");
            static ref RE_ALTER: Regex = Regex::new(r"/ALTER TABLE\w(.+)\w/i")
                .expect("Failed to compile create table regex");
        }

        match MigrationMode::detect(sql) {
            MigrationMode::Alter(table_name) => {
                conn.query_row("SELECT crsql_begin_alter(?)", [&table_name], |_| Ok(()))?;
                conn.execute_batch(sql)?;
                conn.query_row("SELECT crsql_commit_alter(?)", [table_name], |_| Ok(()))?;
            }
            MigrationMode::Create(table_name) => {
                conn.execute_batch(sql)?;
                conn.query_row("SELECT crsql_as_crr(?)", [table_name], |_| Ok(()))?;
            }
            MigrationMode::Other => {
                conn.execute_batch(sql)?;
            }
        }

        Ok(())
    }
}

#[derive(PartialEq, Debug)]
enum MigrationMode {
    Create(String),
    Alter(String),
    Other,
}

impl MigrationMode {
    fn detect(sql: &str) -> Self {
        lazy_static! {
            static ref RE_CREATE: Regex =
                Regex::new("CREATE TABLE \"(.+)\"").expect("Failed to compile create table regex");
            static ref RE_ALTER: Regex =
                Regex::new("ALTER TABLE \"(.+)\"").expect("Failed to compile create table regex");
        }

        if let Some(altered) = RE_ALTER.captures(sql) {
            Self::Alter(altered[1].to_owned())
        } else if let Some(created) = RE_CREATE.captures(sql) {
            Self::Create(created[1].to_owned())
        } else {
            Self::Other
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use axum::{
        extract::{Path, State},
        Json,
    };
    use tracing_test::traced_test;

    use super::{post_migrate, MigratePostData};
    use crate::{
        app_state::{AppEnv, AppState},
        auth::DatabasePermissions,
        database::migrate::MigrationMode,
    };

    #[test]
    fn detect_migration_mode() {
        assert_eq!(
            MigrationMode::detect("CREATE TABLE \"foo\" (value TEXT)"),
            MigrationMode::Create("foo".to_owned())
        );
        assert_eq!(
            MigrationMode::detect("ALTER TABLE \"foo\" ADD COLUMN value TEXT"),
            MigrationMode::Alter("foo".to_owned())
        );
        assert_eq!(
            MigrationMode::detect("INSERT INTO \"foo\" (value) VALUES ('test')"),
            MigrationMode::Other
        );
    }

    pub(crate) fn setup_foo(env: &AppEnv) {
        let migrations =
            vec!["CREATE TABLE \"foo\" (id INTEGER PRIMARY KEY, bar TEXT)".to_string()];

        env.test_db()
            .apply_migrations(&migrations)
            .expect("Failed to apply migrations");
    }

    #[test]
    fn create_simple_table() {
        let env = AppEnv::test_env();
        setup_foo(&env);

        let tables: Vec<String> = env
            .test_db()
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .expect("failed to prepare introspection query")
            .query_map([], |row| row.get(0))
            .expect("failed to read table names")
            .collect::<Result<Vec<String>, rusqlite::Error>>()
            .expect("failed to parse table names");

        assert!(tables.iter().find(|name| *name == "foo").is_some());
    }

    #[traced_test]
    #[tokio::test]
    async fn call_post_endpoint() {
        let state = AppState::test_state();

        post_migrate(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            DatabasePermissions::Full,
            State(state.clone()),
            Json(MigratePostData {
                queries: vec!["CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)".to_string()],
            }),
        )
        .await
        .unwrap();
    }
}
