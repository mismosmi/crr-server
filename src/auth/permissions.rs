use std::collections::HashMap;

use crate::error::CRRError;

#[derive(Default, Debug, Clone)]
struct PartialPermissions {
    read: bool,
    insert: bool,
    update: bool,
    delete: bool,
}

impl PartialPermissions {
    fn grant_read(&mut self) {
        self.read = true;
    }
    fn grant_insert(&mut self) {
        self.insert = true;
    }
    fn grant_update(&mut self) {
        self.update = true;
    }
    fn grant_delete(&mut self) {
        self.delete = true;
    }

    fn is_empty(&self) -> bool {
        return !self.read && !self.insert && !self.update && !self.delete;
    }
}

#[derive(Debug, Clone)]
enum ObjectPermissions {
    Full,
    Partial(PartialPermissions),
}

impl Default for ObjectPermissions {
    fn default() -> Self {
        Self::Partial(PartialPermissions::default())
    }
}

impl ObjectPermissions {
    fn grant_full(&mut self) {
        *self = Self::Full;
    }

    fn grant_read(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial(p) => p.grant_read(),
        }
    }
    fn grant_insert(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial(p) => p.grant_insert(),
        }
    }
    fn grant_update(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial(p) => p.grant_update(),
        }
    }
    fn grant_delete(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial(p) => p.grant_delete(),
        }
    }

    pub(crate) fn full(&self) -> bool {
        match self {
            Self::Full => true,
            _ => false,
        }
    }
    pub(crate) fn read(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.read,
        }
    }
    pub(crate) fn insert(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.insert,
        }
    }
    pub(crate) fn update(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.update,
        }
    }
    pub(crate) fn delete(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.delete,
        }
    }
}

pub(crate) enum DatabasePermissions {
    Full,
    Partial {
        database: PartialPermissions,
        tables: HashMap<String, ObjectPermissions>,
    },
}

impl Default for DatabasePermissions {
    fn default() -> Self {
        Self::Partial {
            database: PartialPermissions::default(),
            tables: HashMap::new(),
        }
    }
}

impl DatabasePermissions {
    pub(crate) fn grant_full(&mut self) {
        *self = Self::Full;
    }
    pub(crate) fn grant_read(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial { database, .. } => {
                database.grant_read();
            }
        }
    }
    pub(crate) fn grant_insert(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial { database, .. } => {
                database.grant_insert();
            }
        }
    }
    pub(crate) fn grant_update(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial { database, .. } => {
                database.grant_update();
            }
        }
    }
    pub(crate) fn grant_delete(&mut self) {
        match self {
            Self::Full => (),
            Self::Partial { database, .. } => {
                database.grant_delete();
            }
        }
    }
    fn with_table<F>(&mut self, table_name: String, f: F)
    where
        F: FnOnce(&mut ObjectPermissions),
    {
        match self {
            Self::Full => (),
            Self::Partial { tables, .. } => {
                let table = tables
                    .entry(table_name)
                    .or_insert(ObjectPermissions::default());

                f(table)
            }
        }
    }

    pub(crate) fn grant_table_full(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.grant_full());
    }
    pub(crate) fn grant_table_read(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.grant_read());
    }
    pub(crate) fn grant_table_insert(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.grant_insert());
    }
    pub(crate) fn grant_table_update(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.grant_update());
    }
    pub(crate) fn grant_table_delete(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.grant_delete());
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Full => false,
            Self::Partial { database, tables } => {
                return database.is_empty() && tables.is_empty();
            }
        }
    }

    pub(crate) fn apply<F>(&self, f: F) -> Result<(), CRRError>
    where
        F: Fn(Option<&str>, ObjectPermissions) -> Result<(), CRRError>,
    {
        match self {
            Self::Full => f(None, ObjectPermissions::Full),
            Self::Partial { database, tables } => {
                f(None, ObjectPermissions::Partial(database.clone()))?;

                for (table_name, permissions) in tables {
                    f(Some(table_name), permissions.clone())?;
                }

                Ok(())
            }
        }
    }

    pub(crate) fn full(&self) -> bool {
        match self {
            Self::Full => true,
            _ => false,
        }
    }

    pub(crate) fn read(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.read,
        }
    }
    pub(crate) fn insert(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.insert,
        }
    }
    pub(crate) fn update(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.update,
        }
    }
    pub(crate) fn delete(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.delete,
        }
    }
    pub(crate) fn full_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { tables, .. } => {
                tables.get(table_name).map(|p| p.read()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn read_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.read || tables.get(table_name).map(|p| p.read()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn update_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.update || tables.get(table_name).map(|p| p.update()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn insert_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.insert || tables.get(table_name).map(|p| p.insert()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn delete_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.delete || tables.get(table_name).map(|p| p.delete()).unwrap_or(false)
            }
        }
    }
}
