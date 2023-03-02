use rusqlite::{named_params, OptionalExtension};

use crate::error::Error;

fn open_db() -> Result<rusqlite::Connection, Error> {
    let conn = rusqlite::Connection::open("./data/migrations.sqlite3")?;

    Ok(conn)
}

pub(crate) fn setup_db() -> Result<(), Error> {
    let conn = open_db()?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            database_name TEXT NOT NULL,
            version INTEGER,
            statements TEXT NOT NULL,
            applied INTEGER DEFAULT FALSE
        );
    ",
    )?;

    Ok(())
}

#[derive(rocket::FromForm)]
pub(crate) struct PostMigrationRequestData<'a> {
    sql: &'a str,
}

#[rocket::post("/<database>/migrations/<version>", data = "<data>")]
pub(crate) fn post_migration(
    database: &str,
    version: i64,
    data: rocket::form::Form<PostMigrationRequestData>,
) -> Result<(), Error> {
    let meta = open_db()?;

    let current_version: i64 = meta
        .prepare(
            "SELECT IFNULL(MAX(version), -1) FROM migrations WHERE database_name = :database_name",
        )?
        .query_row(named_params! { ":database_name": database }, |row| {
            row.get(0)
        })?;

    if version != current_version + 1 {
        return Err(Error::MigrationError(format!("Migration version must be exactly 1 larger than the previous migration. Expected {} received {}", current_version + 1, version)));
    }

    let db = super::open_db(database)?;

    db.execute_batch(data.sql)?;

    meta.prepare("INSERT INTO migrations (database_name, statements, applied) VALUES (:database_name, :statements, TRUE)")?
        .insert(named_params! { ":database_name": database, ":statements": data.sql })?;

    Ok(())
}
