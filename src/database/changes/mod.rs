pub(crate) mod change_manager;
mod changeset;

use base64::{engine::general_purpose::STANDARD as base64, Engine};
use rocket::{
    http::CookieJar,
    response::stream::{Event, EventStream},
    serde::json::Json,
    State,
};
use rusqlite::{named_params, params_from_iter, ToSql};

use crate::{auth::User, error::CRRError, metadata::Metadata};

use super::{Changeset, Database};

const CHANGE_BUFFER_SIZE: usize = 1_000_000;

#[rocket::get("/<database>/changes?<site_id>&<db_version>")]
pub(crate) async fn stream_changes<'s, 'c, 'i>(
    database: String,
    site_id: &'i str,
    db_version: i64,
    change_manager: &'s State<change_manager::ChangeManager>,
    cookies: &'c CookieJar<'c>,
) -> EventStream![Event + 's]
where
    'i: 's,
    'c: 's,
{
    EventStream! {
        let db = Database::open_readonly(database.clone(), db_version);
        let meta = Metadata::open_readonly();
        let site_id = base64.decode(site_id);

        if site_id.is_err() {
            yield Event::json(&Error::ValidationError("Invalid site_id".to_owned()));
            return
        }

        let site_id = site_id.unwrap();

        if let Err(error) = db {
            yield Event::json(&error);
            return
        }

        let mut db = db.unwrap();

        if let Err(error) = meta {
            yield Event::json(&error);
            return
        }

        let meta = meta.unwrap();

        let allowed_tables = User::authenticate(&meta, cookies).and_then(|user| user.readable_tables(&meta, db.name()));

        if let Err(error) = allowed_tables {
            yield Event::json(&error);
            return
        }

        let allowed_tables = allowed_tables.unwrap();

        let changes = db.changes(&allowed_tables, &site_id);

        if let Err(error) = changes {
            yield Event::json(&error);
            return
        }

        let changes = changes.unwrap();

        for changeset in changes {
            match changeset {
                Ok(changeset) => {
                    yield Event::json(&changeset)
                },
                Err(error) => {
                    yield Event::json(&error);
                    return
                }
            }
        }

        let subscription = change_manager.subscribe(db).await;

        if let Err(error) = subscription {
            yield Event::json(&error);
            return
        }

        let mut subscription = subscription.unwrap();

        while let Ok(changeset) = subscription.recv().await {
            match changeset {
                Ok(changeset) => {
                    if !allowed_tables.contains(&changeset.table) {
                        continue
                    }
                    if &changeset.site_id == &site_id {
                        continue
                    }
                    yield Event::json(&changeset)
                },
                Err(error) => {
                    yield Event::json(&error);
                    return
                }
            }
        }
    }
}

#[rocket::post("/<database>/changes", format = "json", data = "<changes>")]
pub(crate) fn post_changes(
    database: String,
    changes: Json<Vec<Changeset>>,
) -> Result<(), CRRError> {
    let mut db = Database::open(database)?;

    db.apply_changes(changes.into_inner())?;

    Ok(())
}

impl Database {
    fn changes<'d, 's, 't>(
        &'d mut self,
        allowed_tables: &'t Vec<String>,
        site_id: &'s Vec<u8>,
    ) -> Result<ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd>, CRRError>
    where
        's: 'd,
        't: 'd,
    {
        Ok(ChangesIter::new(move || {
            let query = format!(
                "
                    SELECT \"table\", pk, cid, val, col_version, db_version, site_id
                    FROM crsql_changes
                    WHERE db_version > ?
                    AND site_id <> ?
                    AND table IN ({})
                ",
                vec!["?"].repeat(allowed_tables.len()).join(", ")
            );

            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;

                let mut stmt = self.prepare(&query)?;

                let mut params: Vec<Box<dyn ToSql>> = Vec::new();

                params.push(Box::new(self.db_version));
                params.push(Box::new(site_id));

                for table_name in allowed_tables {
                    params.push(Box::new(table_name));
                }

                let mut rows = stmt.query(params_from_iter(params.iter()))?;

                while let Ok(Some(row)) = rows.next() {
                    let changeset: Changeset = row.try_into()?;

                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            if let Some(changeset) = buffer.last() {
                self.db_version = changeset.db_version
            }

            Ok((buffer, has_next_page))
        }))
    }

    fn all_changes<'d>(
        &'d mut self,
    ) -> ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd> {
        ChangesIter::new(move || {
            let query = "
                SELECT \"table\", pk, cid, val, col_version, db_version, site_id
                FROM crsql_changes
                WHERE db_version > ?
            ";

            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;
                let mut stmt = self.conn.prepare(query)?;
                let mut rows = stmt.query([&self.db_version])?;

                while let Some(row) = rows.next()? {
                    let changeset: Changeset = row.try_into()?;
                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            if let Some(changeset) = buffer.last() {
                self.db_version = changeset.db_version;
            }

            Ok((buffer, has_next_page))
        })
    }

    fn apply_changes(&mut self, changes: Vec<Changeset>) -> Result<(), CRRError> {
        let query = "
            INSERT INTO crsql_changes (\"table\", pk, cid, val, col_version, db_version, site_id)
            VALUES (:table, :pk, :cid, :val, :col_version, :db_version, :site_id)
        ";

        let mut stmt = self.prepare(query)?;

        for changeset in changes {
            stmt.insert(named_params! {
                ":table": changeset.table,
                ":pk": changeset.pk,
                ":cid": changeset.cid,
                ":val": changeset.val,
                ":col_version": changeset.col_version,
                ":db_version": changeset.db_version,
                ":site_id": changeset.site_id,
            })?;
        }

        Ok(())
    }
}

struct ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    load_page: std::sync::Mutex<F>,
    current_page: <Vec<Changeset> as IntoIterator>::IntoIter,
    has_next_page: bool,
}

impl<F> ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    fn new(load_page: F) -> Self {
        Self {
            load_page: std::sync::Mutex::new(load_page),
            current_page: Vec::new().into_iter(),
            has_next_page: true,
        }
    }
}

impl<F> Iterator for ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    type Item = Result<Changeset, CRRError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(changeset) = self.current_page.next() {
            return Some(Ok(changeset));
        }

        if self.has_next_page {
            match self
                .load_page
                .lock()
                .map_err(|_| CRRError::ServerError("Poisoned lock in ChangesIter".to_owned()))
                .and_then(|mut lock| Ok(lock()?))
            {
                Ok((page, has_next_page)) => {
                    self.current_page = page.into_iter();
                    self.has_next_page = has_next_page;
                    return self.current_page.next().map(|changeset| Ok(changeset));
                }
                Err(error) => return Some(Err(error)),
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        database::{
            changes::change_manager::ChangeManager, migrate::tests::setup_foo, Changeset, Value,
        },
        error::CRRError,
        tests::TestEnv,
    };
    use rocket::tokio;

    #[test]
    fn list_changes() {
        let env = TestEnv::new();
        setup_foo(&env);

        let mut db = env.db();

        {
            let mut changes = db.all_changes();
            assert!(changes.next().is_none());
        }

        db.execute("INSERT INTO foo (bar) VALUES (?)", ["baz"])
            .expect("failed to insert row");

        {
            let mut changes = db.all_changes();
            let changeset = changes
                .next()
                .expect("no changes registered")
                .expect("error fetching changes");

            assert_eq!(changeset.table, "foo");
            assert_eq!(changeset.val, Value::text("baz"));
            assert_eq!(changeset.db_version, 1);

            assert!(changes.next().is_none());
        }

        db.execute("INSERT INTO foo (bar) VALUES (?)", ["foobar"])
            .expect("failed to insert row");

        {
            let mut changes = db.all_changes();
            let changeset = changes
                .next()
                .expect("no changes registered")
                .expect("error fetching changes");

            assert_eq!(changeset.val, Value::text("foobar"));
            assert_eq!(changeset.db_version, 2);
            assert!(changes.next().is_none());
        }
    }

    #[tokio::test]
    async fn react_to_changes() {
        let env = TestEnv::new();
        setup_foo(&env);

        let change_manager = ChangeManager::new(env.meta());

        let mut sub = change_manager
            .subscribe(env.db())
            .await
            .expect("Failed to set up subscription");

        env.db()
            .execute("INSERT INTO foo (bar) VALUES (?)", ["baz"])
            .expect("Failed to insert data");

        let changeset = sub
            .recv()
            .await
            .expect("Failed to receive update")
            .expect("Failed to retrieve updates");

        println!("{:?}", &changeset);

        assert_eq!(changeset.table, "foo")
    }

    #[test]
    fn sync_changes_to_database() {
        let env_a = TestEnv::new();
        setup_foo(&env_a);
        let env_b = TestEnv::new();

        let mut db_a = env_a.db();

        db_a.execute("INSERT INTO foo (bar) VALUES ('baz')", [])
            .expect("Failed to insert stuff");

        let changes = db_a
            .all_changes()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .expect("Failed to retrieve changes");

        let mut db_b = env_b.db();
        db_b.apply_changes(changes)
            .expect("Failed to apply changes");

        let baz: String = db_b
            .query_row("SELECT bar FROM foo", [], |row| row.get(0))
            .expect("Failed to select bar from foo");

        assert_eq!(baz, "baz");
    }
}
