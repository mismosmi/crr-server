use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum_extra::extract::CookieJar;

use crate::{app_state::AppState, auth::AuthDatabase, database::Database, error::CRRError};

use super::Changeset;

pub(crate) async fn post_changes(
    Path(db_name): Path<String>,
    cookies: CookieJar,
    State(state): State<AppState>,
    Json(changes): Json<Vec<Changeset>>,
) -> Result<(), CRRError> {
    let permissions = AuthDatabase::open_readonly(Arc::clone(state.env()))?
        .get_permissions(&cookies, &db_name)?;

    let mut db = Database::open(&state.env(), db_name, permissions)?;

    db.apply_changes(changes)?;

    Ok(())
}
