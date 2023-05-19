use crate::{auth::database::AuthDatabase, error::CRRError};
use axum::{extract::Path, Json};
use axum_extra::extract::CookieJar;
use serde::Deserialize;

use super::Database;

#[derive(Deserialize)]
struct MigratePostData {
    queries: Vec<String>,
}

pub(crate) fn post_migrate(
    db_name: Path<String>,
    data: Json<MigratePostData>,
    cookies: CookieJar,
) -> Result<(), CRRError> {
    AuthDatabase::open_readonly()?.authorize_owned_access(&cookies, &db_name)?;

    let mut db = Database::open(db_name.to_owned())?;

    db.apply_migrations(&data.queries)?;

    Ok(())
}

impl Database {
    fn apply_migrations(&mut self, migrations: &Vec<String>) -> Result<(), CRRError> {
        for migration in migrations {
            self.apply_migration(migration)?;
        }
        Ok(())
    }

    fn apply_migration(&mut self, sql: &str) -> Result<(), CRRError> {
        tracing::info!("Database \"{}\": Applying migration", self.name());

        self.execute_batch(sql)?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::tests::TestEnv;

    pub(crate) fn setup_foo(env: &TestEnv) {
        let mut migrations = vec![
            "CREATE TABLE foo (id INTEGER PRIMARY KEY, bar TEXT); SELECT crsql_as_crr('foo');",
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
