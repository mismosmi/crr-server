use async_stream::try_stream;
use axum::{
    extract::{Path, Query, State},
    response::{sse::Event, Sse},
};
use futures::Stream;
use rusqlite::{params_from_iter, ToSql};
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    auth::{AllowedTables, DatabasePermissions},
    database::Database,
    error::{CRRError, HttpError},
    AppState,
};

use super::{ChangesIter, Changeset, CHANGE_BUFFER_SIZE};

#[derive(Deserialize)]
pub(crate) struct StreamChangesQuery {
    #[serde(with = "crate::serde_base64")]
    site_id: Vec<u8>,
    db_version: i64,
}

pub(crate) async fn stream_changes(
    Path(db_name): Path<String>,
    Query(query): Query<StreamChangesQuery>,
    State(state): State<AppState>,
    permissions: DatabasePermissions,
) -> Result<Sse<impl Stream<Item = Result<Event, HttpError>>>, CRRError> {
    let mut subscription = state
        .change_manager()
        .subscribe(state.env(), &db_name)
        .await?;
    let db = Mutex::new(Database::open_readonly(
        state.env(),
        db_name,
        query.db_version,
        permissions.clone(),
    )?);

    Ok(Sse::new(try_stream! {
        for message in db.lock().await.changes(&query.site_id)? {
            yield Event::try_from(message?)?;
        }

        let mut db_version = db.lock().await.db_version() + 1;
        drop(db);

        while let Ok(message) = subscription.recv().await {
            tracing::debug!("Stream Subscription received Message {:?}", message);
            let changeset = message?;

            if !permissions.read_table(changeset.table()) {
                continue;
            }

            if changeset.db_version() < db_version {
                continue;
            }

            if changeset.site_id() == &query.site_id {
                continue;
            }

            db_version = changeset.db_version();

            yield Event::try_from(changeset)?;
        }

    }))
}

impl Database {
    pub(crate) fn changes<'d, 's>(
        &'d mut self,
        site_id: &'s Vec<u8>,
    ) -> Result<ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd>, CRRError>
    where
        's: 'd,
    {
        let readable_tables = self.permissions().readable_tables();

        if readable_tables.is_empty() {
            return Err(CRRError::Unauthorized(
                "User is not authorized to read database".to_string(),
            ));
        }

        let query = match &readable_tables {
            AllowedTables::All => "
                SELECT \"table\", pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid())
                FROM crsql_changes
                WHERE db_version > ?
                AND site_id IS NOT ?
            ".to_string(),
            AllowedTables::Some(table_names) => format!(
                "
                    SELECT \"table\", pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid())
                    FROM crsql_changes
                    WHERE db_version > ?
                    AND site_id IS NOT ?
                    AND \"table\" IN ({})
                ",
                vec!["?"].repeat(table_names.len()).join(", ")
            ),
        };

        Ok(ChangesIter::new(move || {
            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;
            let mut db_version = self.db_version();

            {
                let mut buffer_size = 0usize;

                let authorized = self.disable_authorization();
                let mut stmt = authorized.prepare(&query)?;

                let mut params: Vec<Box<dyn ToSql>> = Vec::new();

                params.push(Box::new(authorized.db_version()));
                params.push(Box::new(site_id));

                if let AllowedTables::Some(table_names) = &readable_tables {
                    for table_name in table_names {
                        params.push(Box::new(table_name));
                    }
                }

                let mut rows = stmt.query(params_from_iter(params.iter()))?;

                while let Ok(Some(row)) = rows.next() {
                    let changeset: Changeset = row.try_into()?;

                    if buffer_size > CHANGE_BUFFER_SIZE && changeset.db_version() > db_version {
                        has_next_page = true;
                        break;
                    }

                    db_version = changeset.db_version();

                    buffer_size += changeset.size();

                    buffer.push(changeset);
                }
            }

            self.set_db_version(db_version);

            Ok((buffer, has_next_page))
        }))
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use axum::{
        body::{BoxBody, HttpBody},
        extract::{Path, Query, State},
        response::{IntoResponse, Response},
    };

    use crate::{
        app_state::{AppEnv, AppState},
        auth::{DatabasePermissions, PartialPermissions},
        database::{changes::Changeset, migrate::tests::setup_foo, Database, Value},
        error::CRRError,
    };

    use super::stream_changes;

    const SITE_ID: [u8; 16] = [
        113, 203, 3, 166, 76, 47, 79, 47, 178, 78, 194, 120, 89, 221, 198, 42,
    ];

    #[test]
    fn read_simple_changes() {
        let env = AppEnv::test_env();
        setup_foo(&env);
        env.test_db()
            .execute("INSERT INTO foo (bar) VALUES ('foo')", [])
            .unwrap();

        let mut db = env.test_db();

        let changes = db
            .changes(&Vec::from(SITE_ID))
            .unwrap()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .unwrap();

        assert_eq!(changes.len(), 1);
        let row = changes.get(0).unwrap();
        assert_eq!(row.table(), "foo");
    }

    #[test]
    fn with_permissions() {
        let env = AppEnv::test_env();

        env.test_db()
            .apply_migrations(&vec![
                "CREATE TABLE \"foo\" (val TEXT PRIMARY KEY)".to_string(),
                "CREATE TABLE \"bar\" (val TEXT PRIMARY KEY)".to_string(),
                "INSERT INTO foo (val) VALUES ('a')".to_string(),
                "INSERT INTO bar (val) VALUES ('b')".to_string(),
            ])
            .unwrap();

        {
            let db = env.test_db();
            let mut stmt = db
                .prepare("SELECT \"table\" FROM crsql_changes WHERE \"table\" IN ('foo', 'bar')")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();

            while let Ok(Some(row)) = rows.next() {
                let table_name: String = row.get_unwrap("table");
                println!("{:?}", table_name)
            }
        }

        let changes_with_permissions = |permissions: DatabasePermissions| {
            let mut db =
                Database::open(&env, AppEnv::TEST_DB_NAME.to_owned(), permissions).unwrap();

            db.changes(&Vec::from(SITE_ID))
                .and_then(|changes| changes.collect::<Result<Vec<Changeset>, CRRError>>())
        };

        assert!(changes_with_permissions(DatabasePermissions::default()).is_err());

        let changes = changes_with_permissions(DatabasePermissions::Partial {
            database: PartialPermissions {
                read: true,
                insert: false,
                update: false,
                delete: false,
            },
            tables: HashMap::new(),
        })
        .expect("Failed to retrieve changes with database read permission");

        println!("{:?}", changes);

        assert_eq!(changes.get(0).unwrap().table(), "bar");
        assert_eq!(changes.get(0).unwrap().cid(), Some("__crsql_pko"));
        assert_eq!(changes.get(0).unwrap().val(), &Value::Null);
        assert_eq!(changes.get(0).unwrap().pk(), &Value::Text("'b'".to_owned()));

        assert_eq!(changes.get(1).unwrap().table(), "foo");
        assert_eq!(changes.get(1).unwrap().cid(), Some("__crsql_pko"));
        assert_eq!(changes.get(1).unwrap().val(), &Value::Null);
        assert_eq!(changes.get(1).unwrap().pk(), &Value::Text("'a'".to_owned()));
    }

    #[tokio::test]
    async fn stream_simple_changes() {
        let state = AppState::test_state();

        setup_foo(state.env());

        state
            .env()
            .test_db()
            .execute("INSERT INTO foo (bar) VALUES ('foo')", [])
            .unwrap();

        let res: Response = stream_changes(
            Path(AppEnv::TEST_DB_NAME.to_owned()),
            Query(super::StreamChangesQuery {
                site_id: Vec::new(),
                db_version: 0,
            }),
            State(state.clone()),
            DatabasePermissions::Full,
        )
        .await
        .expect("Failed to start stream")
        .into_response();

        let mut body = res.into_body();

        let changeset = read_event(&mut body).await;
        assert_eq!(changeset.table(), "foo");
        assert_eq!(changeset.cid(), Some("bar"));

        assert!(!body.is_end_stream());

        state
            .env()
            .test_db()
            .execute("INSERT INTO foo (bar) VALUES ('bar'), ('baz')", [])
            .unwrap();

        assert_eq!(
            read_event(&mut body).await.val(),
            &Value::Text("'bar'".to_owned())
        );
        assert_eq!(
            read_event(&mut body).await.val(),
            &Value::Text("'baz'".to_owned())
        );
    }

    async fn read_event(body: &mut BoxBody) -> Changeset {
        let event = body
            .data()
            .await
            .expect("Stream is empty")
            .expect("Received Error");

        assert!(event.starts_with("event:change\ndata:".as_bytes()));
        let data = event.slice(18..);
        serde_json::from_slice(&data).expect("Failed to parse response data")
    }
}
