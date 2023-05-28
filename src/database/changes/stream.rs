use std::sync::Arc;

use async_stream::try_stream;
use axum::{
    extract::{Path, Query, State},
    response::{sse::Event, Sse},
};
use axum_extra::extract::CookieJar;
use futures::Stream;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::{
    auth::{AuthDatabase, DatabasePermissions},
    database::Database,
    error::{CRRError, HttpError},
    AppState,
};

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

        let mut db_version = db.lock().await.db_version();
        drop(db);

        while let Ok(message) = subscription.recv().await {
            let changeset = message?;

            if !permissions.read_table(changeset.table()) {
                continue;
            }

            if changeset.db_version() <= db_version {
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

#[cfg(test)]
mod tests {
    use axum::{
        body::HttpBody,
        extract::{Path, Query, State},
        response::{IntoResponse, Response},
    };

    use crate::{
        app_state::{AppEnv, AppState},
        auth::DatabasePermissions,
        database::migrate::tests::setup_foo,
    };

    use super::stream_changes;

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
            State(state),
            DatabasePermissions::Full,
        )
        .await
        .expect("Failed to start stream")
        .into_response();

        let mut body = res.into_body();

        let event = body
            .data()
            .await
            .expect("Stream was empty")
            .expect("Received Error");

        assert_eq!(event, "event:change\ndata:");
    }
}
