mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;

use error::Error;

#[rocket::get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[rocket::main]
async fn main() -> Result<(), Error> {
    dotenv::dotenv()?;

    auth::setup_db()?;
    database::setup_db()?;

    let _rocket = rocket::build()
        .mount("/", rocket::routes![index])
        .mount(
            "/auth",
            rocket::routes![auth::otp, auth::refresh_token, auth::access_token],
        )
        .mount("/database", rocket::routes![database::post_migration])
        .launch()
        .await?;

    Ok(())
}
