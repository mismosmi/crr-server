mod database;
mod migrate;
mod run;
mod value;

use axum::{routing::post, Router};
pub(crate) use database::Database;
pub(crate) use value::Value;

use self::{migrate::post_migrate, run::post_run};

pub(crate) fn router() -> Router {
    Router::new()
        .route("/:db_name/migrate", post(post_migrate))
        .route("/:db_name/run", post(post_run))
}
