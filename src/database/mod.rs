mod migrations;

use crate::error::Error;
pub(crate) use migrations::post_migration;
use rusqlite::LoadExtensionGuard;

pub(crate) fn open_db(database: &str) -> Result<rusqlite::Connection, Error> {
    let conn = rusqlite::Connection::open(format!("./data/database/{}.sqlite3", database))?;

    let os = match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "windows",
        "linux" => "linux",
        os => return Err(Error::ServerError(format!("Unsupported OS: {}", os))),
    };

    let arch = std::env::consts::ARCH;
    let ext = std::env::consts::DLL_EXTENSION;
    let extension_name = format!(
        "./extensions/crsqlite-{os}-{arch}.{ext}",
        os = os,
        arch = arch,
        ext = ext
    );

    unsafe {
        let _guard = LoadExtensionGuard::new(&conn)?;
        conn.load_extension(extension_name, None)?;
    }

    Ok(conn)
}

pub(crate) fn setup_db() -> Result<(), Error> {
    migrations::setup_db()?;

    Ok(())
}
