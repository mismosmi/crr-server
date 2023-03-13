mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;
mod metadata;

use database::changes::ChangeManager;
use error::Error;
use metadata::Metadata;

#[rocket::get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[rocket::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv()?;

    Metadata::open()?.apply_migrations()?;

    let _rocket = rocket::build()
        .manage(ChangeManager::new())
        .mount("/", rocket::routes![index])
        .mount("/auth", rocket::routes![auth::otp::otp, auth::token::token])
        .mount(
            "/database",
            rocket::routes![
                database::migrations::post_migrations,
                database::changes::changes
            ],
        )
        .launch()
        .await?;

    Ok(())
}
