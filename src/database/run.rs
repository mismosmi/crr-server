use axum::{extract::Path, Json};
use axum_extra::extract::CookieJar;
use rusqlite::params_from_iter;
use serde::{Deserialize, Serialize};

use crate::{auth::database::AuthDatabase, error::CRRError};

use super::{Database, Value};

#[derive(Deserialize)]
pub(crate) struct RunPostData {
    sql: String,
    params: Vec<Value>,
    method: String,
}

#[derive(Serialize)]
pub(crate) struct RunPostResponse {
    rows: Vec<Vec<Value>>,
    changes: Option<usize>,
}

pub(crate) async fn post_run(
    cookies: CookieJar,
    Path(db_name): Path<String>,
    Json(data): Json<RunPostData>,
) -> Result<axum::Json<RunPostResponse>, CRRError> {
    let permissions = AuthDatabase::open_readonly()?.get_permissions(&cookies, &db_name)?;

    let db = Database::open(db_name.clone(), permissions)?;

    let mut stmt = db.prepare(&data.sql)?;
    let column_count = stmt.column_count();

    match &data.method[..] {
        "run" => {
            let affected_rows = stmt.execute(params_from_iter(data.params.into_iter()))?;

            Ok(axum::Json(RunPostResponse {
                rows: Vec::new(),
                changes: Some(affected_rows),
            }))
        }
        "get" => {
            let row: Vec<Value> =
                stmt.query_row(params_from_iter(data.params.into_iter()), |raw_row| {
                    let mut row = Vec::new();

                    for i in 0..column_count {
                        row.push(raw_row.get(i)?);
                    }

                    Ok(row)
                })?;

            Ok(axum::Json(RunPostResponse {
                rows: vec![row],
                changes: None,
            }))
        }
        _ => {
            let mut raw_rows = stmt.query(params_from_iter(data.params.into_iter()))?;
            let mut rows = Vec::new();

            while let Some(raw_row) = raw_rows.next()? {
                let mut row = Vec::with_capacity(column_count);

                for i in 0..column_count {
                    row.push(raw_row.get(i)?);
                }

                rows.push(row);
            }

            Ok(axum::Json(RunPostResponse {
                rows,
                changes: None,
            }))
        }
    }
}
