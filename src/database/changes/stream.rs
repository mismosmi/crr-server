use std::convert::Infallible;

use axum::{
    extract::{Path, Query, State},
    response::{sse::Event, Sse},
};
use axum_extra::extract::CookieJar;
use futures::{stream, Stream, StreamExt};
use serde::Deserialize;

use crate::{auth::database::AuthDatabase, database::Database, error::CRRError};

use super::{ChangeManager, Message};

#[derive(Deserialize)]
struct StreamChangesQuery {
    #[serde(with = "crate::serde_base64")]
    site_id: Vec<u8>,
    db_version: i64,
}

pub(crate) async fn stream_changes(
    Path(db_name): Path<String>,
    Query(query): Query<StreamChangesQuery>,
    State(change_manager): State<ChangeManager>,
    cookies: CookieJar,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, CRRError> {
    let permissions = AuthDatabase::open()?.get_permissions(&cookies, &db_name)?;
    let mut db = Database::open_readonly(db_name, query.db_version, permissions)?;

    let changes = db.changes(&query.site_id)?;

    let initialStream = stream::iter(changes).map(|message| Ok(Message::from(message).into()));

    Ok(Sse::new(initialStream))
}
