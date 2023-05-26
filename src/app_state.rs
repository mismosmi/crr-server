use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::database::changes::ChangeManager;

#[derive(Clone)]
pub(crate) struct AppState {
    env: Arc<AppEnv>,
    change_manager: ChangeManager,
}

impl AppState {
    pub(crate) fn init() -> Self {
        Self {
            env: Arc::new(AppEnv::load()),
            change_manager: ChangeManager::new(),
        }
    }

    pub(crate) fn env(&self) -> Arc<AppEnv> {
        Arc::clone(&self.env)
    }

    pub(crate) fn change_manager(&self) -> &ChangeManager {
        &self.change_manager
    }
}

pub(crate) struct AppEnv {
    data_dir: PathBuf,
}

impl AppEnv {
    fn load() -> Self {
        Self {
            data_dir: PathBuf::from(
                std::env::var("CRR_DATA_DIR").unwrap_or_else(|_| "./data".to_owned()),
            ),
        }
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

#[cfg(test)]
impl AppEnv {
    pub(crate) const TEST_DB_NAME: &str = "data";

    pub(crate) fn test_env() -> Arc<Self> {
        use crate::auth::AuthDatabase;

        let mut data_dir = PathBuf::from("./test-data");
        data_dir.push(nanoid::nanoid!());

        let _err = std::fs::create_dir_all(&data_dir);

        let this = Arc::new(Self { data_dir });
        let auth = AuthDatabase::open(Arc::clone(&this)).expect("Failed to open AuthDatabase");

        auth.apply_migrations()
            .expect("Failed to apply metadata migrations");

        this
    }

    pub(crate) fn test_db(&self) -> crate::database::Database {
        use crate::{auth::DatabasePermissions, database::Database};

        Database::open(
            self,
            Self::TEST_DB_NAME.to_owned(),
            DatabasePermissions::Full,
        )
        .expect("Failed to open Test Database")
    }
}

#[cfg(test)]
impl std::ops::Drop for AppEnv {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.data_dir).expect("Failed to clean up test data");
    }
}
