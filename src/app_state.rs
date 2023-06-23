use std::{
    env::temp_dir,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::extract::FromRef;

use crate::database::changes::ChangeManager;

#[derive(Clone)]
pub struct AppState {
    env: Arc<AppEnv>,
    change_manager: ChangeManager,
}

impl AppState {
    pub fn init(disable_validation: bool) -> Self {
        Self {
            env: Arc::new(AppEnv::load(disable_validation)),
            change_manager: ChangeManager::new(),
        }
    }

    pub fn test_state() -> Self {
        Self {
            env: AppEnv::test_env(),
            change_manager: ChangeManager::new(),
        }
    }

    pub fn env(&self) -> &Arc<AppEnv> {
        &self.env
    }

    pub(crate) fn change_manager(&self) -> &ChangeManager {
        &self.change_manager
    }
}

pub struct AppEnv {
    data_dir: PathBuf,
    disable_validation: bool,
}

impl AppEnv {
    pub(crate) const TEST_DB_NAME: &str = "data";

    fn load(disable_validation: bool) -> Self {
        Self {
            data_dir: PathBuf::from(
                std::env::var("CRR_DATA_DIR").unwrap_or_else(|_| "./data".to_owned()),
            ),
            disable_validation,
        }
    }

    pub(crate) fn test_env() -> Arc<Self> {
        use crate::auth::AuthDatabase;

        let mut data_dir = PathBuf::from(temp_dir());
        data_dir.push("crr-test-data");
        data_dir.push(nanoid::nanoid!());

        tracing::info!("Created Test Env at {}", data_dir.display());

        let _err = std::fs::create_dir_all(&data_dir);

        let app_env = Arc::new(AppEnv {
            data_dir,
            disable_validation: false,
        });
        let auth = AuthDatabase::open(Arc::clone(&app_env)).expect("Failed to open AuthDatabase");

        auth.apply_migrations()
            .expect("Failed to apply metadata migrations");

        app_env
    }

    pub(crate) fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub(crate) fn disable_validation(&self) -> bool {
        self.disable_validation
    }

    pub fn test_db(&self) -> crate::database::Database {
        use crate::{auth::DatabasePermissions, database::Database};

        Database::open(
            self,
            Self::TEST_DB_NAME.to_owned(),
            DatabasePermissions::Full,
        )
        .expect("Failed to open Test Database")
    }
}

impl FromRef<AppState> for Arc<AppEnv> {
    fn from_ref(input: &AppState) -> Self {
        Arc::clone(input.env())
    }
}
