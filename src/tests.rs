use crate::{
    auth::{database::AuthDatabase, DatabasePermissions},
    database::Database,
};

#[derive(Clone)]
pub(crate) struct TestEnv {
    folder: std::path::PathBuf,
}

impl TestEnv {
    pub(crate) const DB_NAME: &'static str = "data.sqlite3";

    pub(crate) fn new() -> Self {
        let this = Self {
            folder: format!("./test-data/{}", nanoid::nanoid!()).into(),
        };

        let _err = std::fs::create_dir_all(&this.folder);

        let auth = AuthDatabase::open_for_test(&this);

        auth.apply_migrations()
            .expect("Failed to apply metadata migrations");

        this
    }

    pub(crate) fn folder(&self) -> &std::path::Path {
        &self.folder
    }

    pub(crate) fn auth(&self) -> AuthDatabase {
        AuthDatabase::open_for_test(self)
    }

    pub(crate) fn db(&self) -> Database {
        Database::open_for_test(self, DatabasePermissions::Full)
    }
}

impl std::ops::Drop for TestEnv {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.folder).expect("Failed to clean up test data");
    }
}
