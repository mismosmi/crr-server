use crate::{auth::database::AuthDatabase, error::CRRError, AppState};
use axum::extract::{Json, Path, State};
use axum_extra::extract::CookieJar;
use serde::Deserialize;

use super::Database;

#[derive(Deserialize)]
pub(crate) struct MigratePostData {
    queries: Vec<String>,
}

pub(crate) async fn post_migrate(
    Path(db_name): Path<String>,
    cookies: CookieJar,
    State(state): State<AppState>,
    Json(data): Json<MigratePostData>,
) -> Result<(), CRRError> {
    let permissions = AuthDatabase::open(state.env())?.authorize_migration(&cookies, &db_name)?;

    state.change_manager().kill_connection(&db_name).await;

    let mut db = Database::open(&state.env(), db_name, permissions)?;

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
    use crate::app_state::AppEnv;

    pub(crate) fn setup_foo(env: &AppEnv) {
        let migrations = vec![
            "CREATE TABLE foo (id INTEGER PRIMARY KEY, bar TEXT); SELECT crsql_as_crr('foo');"
                .to_string(),
        ];

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
}
