use axum::extract::{Json, Path};
use axum_extra::extract::CookieJar;

use crate::{auth::database::AuthDatabase, database::Database, error::CRRError};

use super::Changeset;

pub(crate) async fn post_changes(
    Path(db_name): Path<String>,
    cookies: CookieJar,
    Json(changes): Json<Vec<Changeset>>,
) -> Result<(), CRRError> {
    let permissions = AuthDatabase::open_readonly()?.get_permissions(&cookies, &db_name)?;

    let mut db = Database::open(db_name, permissions)?;

    db.apply_changes(changes)?;

    Ok(())
}
