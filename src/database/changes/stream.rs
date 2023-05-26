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
    auth::database::AuthDatabase,
    database::Database,
    error::{CRRError, HttpError},
};

use super::ChangeManager;

#[derive(Deserialize)]
pub(crate) struct StreamChangesQuery {
    #[serde(with = "crate::serde_base64")]
    site_id: Vec<u8>,
    db_version: i64,
}

pub(crate) async fn stream_changes(
    Path(db_name): Path<String>,
    Query(query): Query<StreamChangesQuery>,
    State(change_manager): State<ChangeManager>,
    cookies: CookieJar,
) -> Result<Sse<impl Stream<Item = Result<Event, HttpError>>>, CRRError> {
    let permissions = AuthDatabase::open()?.get_permissions(&cookies, &db_name)?;
    let mut subscription = change_manager.subscribe(&db_name).await?;
    let db = Mutex::new(Database::open_readonly(
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
