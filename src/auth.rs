use rocket::http::{Cookie, CookieJar};
use rusqlite::named_params;

use crate::{error::Error, mail::send_email};

fn open_db() -> Result<rusqlite::Connection, Error> {
    let conn = rusqlite::Connection::open("./data/auth.sqlite3")?;

    Ok(conn)
}

pub(crate) fn setup_db() -> Result<(), Error> {
    let conn = open_db()?;

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            otp TEXT
        );
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        );
        CREATE TABLE IF NOT EXISTS user_roles (
            user_id INTEGER,
            role_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (role_id) REFERENCES roles (id),
            PRIMARY KEY (user_id, role_id)
        );
        CREATE TABLE IF NOT EXISTS permissions (
            role_id INTEGER, 
            database_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            pread BOOLEAN NOT NULL,
            pupdate BOOLEAN NOT NULL,
            pinsert BOOLEAN NOT NULL,
            pdelete BOOLEAN NOT NULL,
            FOREIGN KEY (role_id) REFERENCES roles (id),
            PRIMARY KEY (role_id, database_name, table_name)
        );
        CREATE TABLE IF NOT EXISTS access_tokens (
            user_id INTEGER,
            token TEXT PRIMARY KEY,
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        CREATE TABLE IF NOT EXISTS refresh_tokens (
            user_id INTEGER,
            token TEXT PRIMARY KEY,
            create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
    ",
    )?;

    Ok(())
}

#[derive(rocket::FromForm)]
pub(crate) struct OtpRequestData {
    email: String,
}

#[rocket::post("/otp", data = "<data>")]
pub(crate) fn otp(data: rocket::form::Form<OtpRequestData>) -> Result<(), Error> {
    let conn = open_db()?;

    let otp = nanoid::nanoid!();

    let mut stmt = conn.prepare(
        "
        INSERT INTO users (email, otp)
        VALUES (:email, :otp)
        ON CONFLICT (email) DO UPDATE SET otp = :otp;
    ",
    )?;

    stmt.execute(rusqlite::named_params! { ":email": data.email, ":otp": otp})?;

    send_email(&data.email, "Your OTP".to_owned(), otp)?;

    Ok(())
}

#[derive(rocket::FromForm)]
pub(crate) struct RefreshTokenRequestData {
    otp: String,
}

#[rocket::post("/refresh_token", data = "<data>")]
pub(crate) fn refresh_token(
    data: rocket::form::Form<RefreshTokenRequestData>,
    cookies: &CookieJar<'_>,
) -> Result<(), Error> {
    let conn = open_db()?;

    let user_id: i64 = conn
        .prepare("SELECT id FROM users WHERE otp = :otp")?
        .query_row(named_params! { ":otp": data.otp}, |row| row.get(0))?;

    {
        let refresh_token = nanoid::nanoid!();

        conn.prepare("INSERT INTO refresh_tokens (user_id, token) VALUES (:user_id, :token)")?
            .insert(named_params! { ":user_id": user_id, ":token": refresh_token })?;

        let cookie = Cookie::build("VLCN_REFRESH_TOKEN", refresh_token)
            .http_only(true)
            .max_age(rocket::time::Duration::days(400))
            .same_site(rocket::http::SameSite::Strict)
            .secure(true)
            .path("/")
            .finish();

        cookies.add(cookie);
    }

    conn.prepare("UPDATE users SET otp = NULL WHERE id = :user_id AND otp = :otp")?
        .execute(named_params! { ":user_id": user_id, ":otp": data.otp })?;

    Ok(())
}

#[rocket::post("/access_token")]
pub(crate) fn access_token(cookies: &CookieJar<'_>) -> Result<(), Error> {
    let conn = open_db()?;

    let cookie = cookies
        .get("VLCN_REFRESH_TOKEN")
        .ok_or(Error::Unauthorized("Refresh Token not found".to_owned()))?;

    let refresh_token = cookie.value();

    let user_id: i64 = conn
        .prepare("SELECT user_id FROM refresh_tokens WHERE token = :token AND julianday(create_date) + 400 > julianday('now')")?
        .query_row(named_params! { ":token": refresh_token }, |row| row.get(0))?;

    {
        let access_token = nanoid::nanoid!();

        conn.prepare("INSERT INTO access_tokens (user_id, token) VALUES (:user_id, :token)")?
            .insert(named_params! { ":user_id": user_id, ":token": access_token })?;

        let cookie = Cookie::build("VLCN_ACCESS_TOKEN", access_token)
            .http_only(true)
            .max_age(rocket::time::Duration::days(7))
            .same_site(rocket::http::SameSite::Strict)
            .secure(true)
            .path("/")
            .finish();

        cookies.add(cookie);
    }

    Ok(())
}
