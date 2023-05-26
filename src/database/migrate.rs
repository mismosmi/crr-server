use std::{fs::Permissions, path::PathBuf};

use crate::{
    auth::{database::AuthDatabase, DatabasePermissions},
    error::CRRError,
};
use axum::extract::{Json, Path, State};
use axum_extra::extract::CookieJar;
use lettre::message;
use regex::Regex;
use serde::Deserialize;

use super::{changes::ChangeManager, Database};

#[derive(Deserialize)]
pub(crate) struct MigratePostData {
    queries: Vec<String>,
}

pub(crate) async fn post_migrate(
    Path(db_name): Path<String>,
    cookies: CookieJar,
    State(change_manager): State<ChangeManager>,
    Json(data): Json<MigratePostData>,
) -> Result<(), CRRError> {
    let permissions = AuthDatabase::open()?.authorize_migration(&cookies, &db_name)?;

    change_manager.kill_connection(&db_name).await;

    let mut db = Database::open(db_name, permissions)?;

    db.apply_migrations(&data.queries)?;

    Ok(())
}

impl Database {
    fn apply_migrations(&mut self, migrations: &Vec<String>) -> Result<(), CRRError> {
        if !self.permissions().full() {
            return Err(CRRError::Unauthorized(
                "User must be authorized with full access to the database to apply migrations"
                    .to_owned(),
            ));
        }

        for migration in migrations {
            self.apply_migration(migration)?;
        }
        Ok(())
    }

    fn apply_migration(&mut self, sql: &str) -> Result<(), CRRError> {
        tracing::info!("Database \"{}\": Applying migration", self.name());

        // TODO auto-apply crsqlite stuff
        self.execute_batch(sql)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::tests::TestEnv;

    pub(crate) fn setup_foo(env: &TestEnv) {
        let migrations = vec![
            "CREATE TABLE foo (id INTEGER PRIMARY KEY, bar TEXT); SELECT crsql_as_crr('foo');"
                .to_string(),
        ];

        env.db()
            .apply_migrations(&migrations)
            .expect("Failed to apply migrations");
    }

    #[test]
    fn create_simple_table() {
        let env = TestEnv::new();
        setup_foo(&env);

        let tables: Vec<String> = env
            .db()
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .expect("failed to prepare introspection query")
            .query_map([], |row| row.get(0))
            .expect("failed to read table names")
            .collect::<Result<Vec<String>, rusqlite::Error>>()
            .expect("failed to parse table names");

        assert!(tables.iter().find(|name| *name == "foo").is_some());
    }
}
