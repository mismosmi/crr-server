mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;
mod metadata;
mod serde_base64;
#[cfg(test)]
mod tests;

use database::changes::change_manager::ChangeManager;
use error::Error;
use metadata::Metadata;

#[rocket::get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[rocket::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv()?;

    let meta = Metadata::open()?;

    meta.apply_migrations()?;

    let _rocket = rocket::build()
        .manage(ChangeManager::new(meta))
        .mount("/", rocket::routes![index])
        .mount("/auth", rocket::routes![auth::otp::otp, auth::token::token])
        .mount(
            "/database",
            rocket::routes![
                database::migrations::post_migrations,
                database::changes::stream_changes,
                database::changes::post_changes
            ],
        )
        .launch()
        .await?;

    Ok(())
}
