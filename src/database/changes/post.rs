use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum_extra::extract::CookieJar;
use rusqlite::named_params;

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

impl Database {
    pub(crate) fn apply_changes(&mut self, changes: Vec<Changeset>) -> Result<(), CRRError> {
        let query = "
            INSERT INTO crsql_changes (\"table\", pk, cid, val, col_version, db_version, site_id)
            VALUES (:table, :pk, :cid, :val, :col_version, :db_version, :site_id)
        ";

        let authorized = self.disable_authorization();

        let mut stmt = authorized.prepare(query)?;

        for changeset in changes {
            if changeset.cid().is_none()
                && !authorized.permissions().delete_table(changeset.table())
            {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to delete from table {}",
                    changeset.table()
                )));
            } else if changeset.col_version() == 1
                && !authorized.permissions().insert_table(changeset.table())
            {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to insert into table {}",
                    changeset.table()
                )));
            } else if !authorized.permissions().update_table(changeset.table()) {
                return Err(CRRError::Unauthorized(format!(
                    "User is not authorized to update table {}",
                    changeset.table()
                )));
            }

            stmt.insert(named_params! {
                ":table": changeset.table(),
                ":pk": changeset.pk(),
                ":cid": changeset.cid(),
                ":val": changeset.val(),
                ":col_version": changeset.col_version(),
                ":db_version": changeset.db_version(),
                ":site_id": changeset.site_id(),
            })?;
        }

        Ok(())
    }
}
