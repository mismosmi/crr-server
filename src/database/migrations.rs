use std::collections::HashMap;

use crate::{error::CRRError, metadata::Metadata};
use rocket::{
    serde::{self, Deserialize},
    FromForm,
};
use rusqlite::named_params;

use super::Database;

#[rocket::post("/<database>/migrations", data = "<data>")]
pub(crate) fn post_migrations(
    database: &str,
    data: rocket::form::Form<HashMap<&str, &str>>,
    cookies: &rocket::http::CookieJar,
) -> Result<(), CRRError> {
    {
        let meta = Metadata::open()?;
        let user = crate::auth::User::authenticate(&meta, cookies)?;

        let granted = user.owns_database(&meta, database)?;

        if !granted {
            return Err(CRRError::Unauthorized(format!(
                "User does not own database \"{}\"",
                database
            )));
        }
    }

    let mut db = Database::open(database.to_owned())?;

    db.apply_migrations(&meta, &data)?;

    Ok(())
}

#[derive(FromForm, Deserialize)]
#[serde(crate = "rocket::serde")]
pub(crate) struct PostMigrationsData {
    queries: Vec<String>,
}

impl Database {
    fn apply_migrations(
        &mut self,
        meta: &Metadata,
        migrations: &HashMap<&str, &str>,
    ) -> Result<(), CRRError> {
        let mut latest_version: Option<i64> = meta
            .prepare("SELECT MAX(version) FROM migrations WHERE database_name = :database_name")?
            .query_row(named_params! { ":database_name": self.name() }, |row| {
                row.get::<usize, Option<i64>>(0)
            })?;

        let mut keys: Vec<&&str> = migrations.keys().collect();
        keys.sort();

        for key in keys {
            if let Ok(version) = key.parse::<i64>() {
                if let Some(latest_version) = latest_version {
                    if version <= latest_version {
                        continue;
                    }
                }

                self.apply_migration(meta, version, migrations.get(key).unwrap())?;
                latest_version = Some(version);
            }
        }
        Ok(())
    }

    fn apply_migration(
        &mut self,
        meta: &Metadata,
        version: i64,
        sql: &str,
    ) -> Result<(), CRRError> {
        println!(
            "Database \"{}\": Applying migration version {}",
            self.name(),
            version
        );
        self.execute_batch(sql)?;

        meta
            .prepare("INSERT INTO migrations (database_name, version, statements) VALUES (:database_name, :version, :statements)")?
            .insert(named_params! { ":version": version, ":statements": sql, ":database_name": self.name() })?;

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::tests::TestEnv;
    use std::collections::HashMap;

    pub(crate) fn setup_foo(env: &TestEnv) {
        let mut migrations = HashMap::new();
        migrations.insert(
            "001",
            "CREATE TABLE foo (id INTEGER PRIMARY KEY, bar TEXT); SELECT crsql_as_crr('foo');",
        );

        env.db()
            .apply_migrations(&env.meta(), &migrations)
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
