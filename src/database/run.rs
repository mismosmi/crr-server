use axum::{
    extract::{Path, State},
    Json,
};
use rusqlite::params_from_iter;
use serde::{Deserialize, Serialize};

use crate::{app_state::AppState, auth::DatabasePermissions, error::CRRError};

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
    Path(db_name): Path<String>,
    State(state): State<AppState>,
    permissions: DatabasePermissions,
    Json(data): Json<RunPostData>,
) -> Result<axum::Json<RunPostResponse>, CRRError> {
    let db = Database::open(&state.env(), db_name.clone(), permissions)?;

    let mut stmt = db.prepare(&data.sql)?;
    let column_count = stmt.column_count();

    tracing::debug!("{} {}", &data.method, &data.sql);

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

#[cfg(test)]
mod tests {
    use axum::{
        extract::{Path, State},
        Json,
    };

    use crate::{
        app_state::{AppEnv, AppState},
        auth::DatabasePermissions,
    };

    use super::{post_run, RunPostData};

    #[tokio::test]
    async fn post_create_table() {
        let state = AppState::test_state();

        let Json(res) = post_run(
            Path(AppEnv::TEST_DB_NAME.to_string()),
            State(state.clone()),
            DatabasePermissions::Full,
            Json(RunPostData {
                sql: "CREATE TABLE test (val TEXT PRIMARY KEY)".to_owned(),
                params: Vec::new(),
                method: "run".to_owned(),
            }),
        )
        .await
        .unwrap();

        assert!(res.rows.is_empty());
        assert_eq!(res.changes, Some(1));
    }
}
