use axum::{extract::Path, Json};
use axum_extra::extract::CookieJar;
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    params_from_iter,
};

use crate::{auth::database::AuthDatabase, error::CRRError};

use super::{Database, Value};

struct RunPostData {
    sql: String,
    params: Vec<serde_json::Value>,
    method: String,
}

pub(crate) async fn post_run(
    cookies: CookieJar,
    Path(db_name): Path<String>,
    Json(data): Json<RunPostData>,
) -> Result<(), CRRError> {
    let permissions = AuthDatabase::open_readonly()?.get_permissions(&cookies, &db_name)?;

    let db = Database::open(db_name.clone())?;

    if !permissions.full() {
        db.authorizer(Some(move |context: AuthContext| match context.action {
            AuthAction::Select => Authorization::Allow,
            AuthAction::Read { table_name, .. } => auth(permissions.read_table(table_name)),
            AuthAction::Update { table_name, .. } => auth(permissions.update_table(table_name)),
            AuthAction::Insert { table_name } => auth(permissions.insert_table(table_name)),
            AuthAction::Delete { table_name } => auth(permissions.delete_table(table_name)),
            AuthAction::Transaction { operation } => Authorization::Allow,
            _ => Authorization::Deny,
        }));
    }

    let mut stmt = db.prepare(&data.sql)?;

    let parsed_params = data
        .params
        .into_iter()
        .map(|param| param.try_into())
        .collect::<Result<Vec<Value>, CRRError>>()?;

    stmt.execute(params_from_iter(parsed_params.into_iter()))?;

    Ok(())
}

fn auth(value: bool) -> Authorization {
    if value {
        Authorization::Allow
    } else {
        Authorization::Deny
    }
}
