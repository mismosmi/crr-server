use crate::{auth::DatabasePermissions, error::CRRError, AppState};
use axum::extract::{Json, Path, State};
use lazy_static::lazy_static;
use regex::Regex;
use serde::Deserialize;

use super::{changes::Migration, Database};

#[derive(Deserialize)]
pub(crate) struct MigratePostData {
    pub(crate) queries: Vec<String>,
}

pub(crate) async fn post_migrate(
    Path(db_name): Path<String>,
    permissions: DatabasePermissions,
    State(state): State<AppState>,
    Json(data): Json<MigratePostData>,
) -> Result<(), CRRError> {
    let mut db = Database::open(&state.env(), db_name.clone(), permissions)?;

    let migration = db.apply_migration(data.queries)?;

    state
        .change_manager()
        .publish_migration(&db_name, migration)
        .await;

    Ok(())
}

impl Database {
    pub(crate) fn apply_migration(
        &mut self,
        migrations: Vec<String>,
    ) -> Result<Migration, CRRError> {
        if !self.permissions().full() {
            return Err(CRRError::Unauthorized(
                "User must be authorized with full access to the database to apply migrations"
                    .to_owned(),
            ));
        }

        let mut crr_migrations: Vec<String> = Vec::with_capacity(migrations.len() * 3 + 2);

        for migration in migrations.into_iter() {
            Self::enable_migration_crr(&mut crr_migrations, migration);
        }

        let joined_migrations: String = crr_migrations.join(";\n");

        tracing::debug!("Run Migration\n{}", joined_migrations);

        let savepoint = self.savepoint()?;

        savepoint.execute_batch(&joined_migrations)?;

        savepoint
            .prepare("INSERT INTO crr_server_migrations (sql) VALUES (?)")?
            .insert([&joined_migrations])?;

        savepoint.commit()?;

        Ok(Migration::new(self.last_insert_rowid(), joined_migrations))
    }

    fn enable_migration_crr(crr_migrations: &mut Vec<String>, sql: String) {
        match MigrationType::detect(&sql) {
            MigrationType::Alter(table_name) => {
                crr_migrations.push(format!("SELECT crsql_begin_alter('{}')", &table_name));
                crr_migrations.push(sql);
                crr_migrations.push(format!("SELECT crsql_commit_alter('{}')", table_name));
            }
            MigrationType::Create(table_name) => {
                crr_migrations.push(sql);
                crr_migrations.push(format!("SELECT crsql_as_crr('{}')", table_name));
            }
            MigrationType::Other => {
                crr_migrations.push(sql);
            }
        }
    }

    pub(crate) fn migrations(&self, schema_version: i64) -> Result<Vec<Migration>, CRRError> {
        let mut stmt =
            self.prepare("SELECT version, \"sql\" FROM crr_server_migrations WHERE version > ?")?;

        let mut rows = stmt.query([schema_version])?;

        let mut migrations = Vec::new();

        while let Some(row) = rows.next()? {
            migrations.push(Migration::new(row.get(0)?, row.get(1)?));
        }

        Ok(migrations)
    }
}

#[derive(PartialEq, Debug)]
enum MigrationType {
    Create(String),
    Alter(String),
    Other,
}

impl MigrationType {
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
        database::migrate::MigrationType,
    };

    #[test]
    fn detect_migration_mode() {
        assert_eq!(
            MigrationType::detect("CREATE TABLE \"foo\" (value TEXT)"),
            MigrationType::Create("foo".to_owned())
        );
        assert_eq!(
            MigrationType::detect("ALTER TABLE \"foo\" ADD COLUMN value TEXT"),
            MigrationType::Alter("foo".to_owned())
        );
        assert_eq!(
            MigrationType::detect("INSERT INTO \"foo\" (value) VALUES ('test')"),
            MigrationType::Other
        );
    }

    pub(crate) fn setup_foo(env: &AppEnv) {
        let migrations =
            vec!["CREATE TABLE \"foo\" (id INTEGER PRIMARY KEY, bar TEXT)".to_string()];

        env.test_db()
            .apply_migration(migrations)
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
            DatabasePermissions::Create,
            State(state.clone()),
            Json(MigratePostData {
                queries: vec![
                    "CREATE TABLE \"test\" (id INTEGER PRIMARY KEY, val TEXT)".to_string()
                ],
            }),
        )
        .await
        .unwrap();
    }
}
