use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct TablePermissions {
    read: bool,
    insert: bool,
    update: bool,
    delete: bool,
}

impl TablePermissions {
    pub(crate) fn merge_read(&mut self, p: bool) {
        self.read = self.read || p;
    }

    pub(crate) fn merge_insert(&mut self, p: bool) {
        self.insert = self.insert || p;
    }

    pub(crate) fn merge_update(&mut self, p: bool) {
        self.update = self.update || p;
    }

    pub(crate) fn merge_delete(&mut self, p: bool) {
        self.delete = self.delete || p;
    }
}

pub(crate) enum DatabasePermissions {
    Full,
    Partial(HashMap<String, TablePermissions>),
}
