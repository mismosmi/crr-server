use crate::{error::Error, metadata::Metadata};

#[derive(rocket::FromForm)]
pub(crate) struct OtpRequestData {
    email: String,
}

#[rocket::post("/otp", data = "<data>")]
pub(crate) fn otp(data: rocket::form::Form<OtpRequestData>) -> Result<(), Error> {
    let metadata = Metadata::open()?;

    let otp = nanoid::nanoid!();

    let mut stmt = metadata.prepare(
        "
        INSERT INTO users (email, otp)
        VALUES (:email, :otp)
        ON CONFLICT (email) DO UPDATE SET otp = :otp;
    ",
    )?;

    stmt.insert(rusqlite::named_params! { ":email": data.email, ":otp": otp})?;

    crate::mail::send_email(&data.email, "Your OTP".to_owned(), otp)?;

    Ok(())
}
