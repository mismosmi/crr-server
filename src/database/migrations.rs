use std::collections::HashMap;

use crate::{error::Error, metadata::Metadata};
use rusqlite::named_params;

use super::Database;

#[rocket::post("/<database>/migrations", data = "<data>")]
pub(crate) fn post_migrations(
    database: &str,
    data: rocket::form::Form<HashMap<&str, &str>>,
    cookies: &rocket::http::CookieJar,
) -> Result<(), Error> {
    let user = crate::auth::User::authenticate(cookies)?;

    let granted = user.owns_database(database)?;

    if !granted {
        return Err(Error::Unauthorized(format!(
            "User does not own database \"{}\"",
            database
        )));
    }

    let mut db = Database::open(database.to_owned())?;

    db.apply_migrations(&data)?;

    Ok(())
}

impl Database {
    fn apply_migrations(&mut self, migrations: &HashMap<&str, &str>) -> Result<(), Error> {
        let mut metadata = Metadata::open()?;

        let mut latest_version: Option<i64> = metadata
            .prepare("SELECT MAX(version) FROM migrations WHERE database_name = :database_name")?
            .query_row(named_params! { ":database_name": self.name() }, |row| {
                row.get(0)
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

                self.apply_migration(&mut metadata, version, migrations.get(key).unwrap())?;
                latest_version = Some(version);
            }
        }
        Ok(())
    }

    fn apply_migration(
        &mut self,
        metadata: &mut Metadata,
        version: i64,
        sql: &str,
    ) -> Result<(), Error> {
        println!(
            "Database \"{}\": Applying migration version {}",
            self.name(),
            version
        );
        self.execute_batch(sql)?;

        metadata
            .prepare("INSERT INTO migrations (version, statements) VALUES (:version, :statements)")?
            .insert(named_params! { ":version": version, ":statements": sql })?;

        Ok(())
    }
}
