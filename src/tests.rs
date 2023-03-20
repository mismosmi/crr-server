use crate::{database::Database, metadata::Metadata};

pub(crate) struct TestEnv {
    folder: std::path::PathBuf,
}

impl TestEnv {
    pub(crate) fn new() -> Self {
        let this = Self {
            folder: format!("./test-data/{}", nanoid::nanoid!()).into(),
        };

        let _err = std::fs::create_dir_all(&this.folder);

        let meta = Metadata::open_for_test(&this);

        meta.apply_migrations()
            .expect("Failed to apply metadata migrations");

        this
    }

    pub(crate) fn folder(&self) -> &std::path::Path {
        &self.folder
    }

    pub(crate) fn meta(&self) -> Metadata {
        Metadata::open_for_test(self)
    }

    pub(crate) fn db(&self) -> Database {
        Database::open_for_test(self)
    }
}

impl std::ops::Drop for TestEnv {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.folder).expect("Failed to clean up test data");
    }
}
