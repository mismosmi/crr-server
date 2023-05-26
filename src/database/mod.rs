pub(crate) mod changes;
mod database;
mod migrate;
mod run;
mod value;

use axum::{
    routing::{get, post},
    Router,
};
pub(crate) use database::Database;
pub(crate) use value::Value;

use self::{
    changes::{post_changes, stream_changes, ChangeManager},
    migrate::post_migrate,
    run::post_run,
};

pub(crate) fn router() -> Router {
    Router::new()
        .route("/:db_name/migrate", post(post_migrate))
        .route("/:db_name/run", post(post_run))
        .route("/:db_name/changes", get(stream_changes).post(post_changes))
        .with_state(ChangeManager::new())
}
