use std::{collections::hash_map::Entry, sync::Arc};

use tokio::sync::broadcast::{self, error::SendError};

use crate::{app_state::AppEnv, auth::DatabasePermissions, database::Database, error::CRRError};

use super::{ChangesIter, Changeset, DatabaseHandle, Message, Subscription, CHANGE_BUFFER_SIZE};

#[derive(Clone)]
pub(crate) struct ChangeManager(
    Arc<tokio::sync::RwLock<std::collections::HashMap<String, DatabaseHandle>>>,
);

impl ChangeManager {
    pub(crate) fn new() -> Self {
        let handles = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::<
            String,
            DatabaseHandle,
        >::new()));

        // garbage collection for database handles
        let gc_handles = Arc::downgrade(&handles);

        tokio::spawn(async move {
            loop {
                tracing::debug!("Run GC");
                tokio::time::sleep(tokio::time::Duration::from_secs(240)).await;

                match gc_handles.upgrade() {
                    Some(handles) => {
                        let mut lock = handles.write().await;

                        let mut collect = Vec::new();
                        for (db_name, handle) in lock.iter() {
                            if handle.is_orphan() {
                                collect.push(db_name.to_owned());
                            }
                        }

                        for db_name in collect.into_iter() {
                            lock.remove(&db_name);
                        }
                    }
                    None => return,
                }

                tracing::debug!("GC Done");
            }
        });

        Self(handles)
    }

    pub(crate) async fn subscribe(
        &self,
        env: &AppEnv,
        db_name: &str,
    ) -> Result<Subscription, CRRError> {
        if let Some(handle) = self.0.read().await.get(db_name) {
            return Ok(handle.subscribe());
        }

        match self.0.write().await.entry(db_name.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().subscribe()),
            Entry::Vacant(entry) => {
                let database = Database::open_readonly_latest(
                    env,
                    db_name.to_owned(),
                    DatabasePermissions::Full,
                )?;
                let (handle, subscription) = Self::add_handle(database).await?;
                entry.insert(handle);

                Ok(subscription)
            }
        }
    }

    async fn add_handle(
        mut database: Database,
    ) -> Result<(DatabaseHandle, Subscription), CRRError> {
        tracing::info!(
            "Start new Database Watcher Task for \"{}\"",
            database.name()
        );
        let (message_sender, message_receiver) = tokio::sync::broadcast::channel::<Message>(32);
        let (signal_sender, mut signal_receiver) = tokio::sync::mpsc::channel::<()>(1);

        let hook_signal_sender = signal_sender.downgrade();

        database.update_hook(Some(
            move |_action, _arg1: &'_ str, _arg2: &'_ str, rowid| {
                tracing::debug!("update hook triggered for row {}", rowid);
                if let Some(sender) = hook_signal_sender.upgrade() {
                    let _ = sender.try_send(());
                }
            },
        ));

        let task_message_sender = message_sender.clone();

        tokio::spawn(async move {
            if let Err(_) = Self::send_changes(&mut database, &task_message_sender) {
                // no receivers, stop this task
                return;
            }

            while let Some(_) = signal_receiver.recv().await {
                if let Err(_) = Self::send_changes(&mut database, &task_message_sender) {
                    // no receivers, stop this task
                    return;
                }
            }
        });

        let handle = DatabaseHandle::from(message_sender, signal_sender);

        Ok((handle, message_receiver))
    }

    fn send_changes(
        database: &mut Database,
        sender: &broadcast::Sender<Message>,
    ) -> Result<(), SendError<Message>> {
        for message in database.all_changes() {
            sender.send(message.map_err(Into::into))?;
        }

        Ok(())
    }

    pub(crate) async fn kill_connection(&self, db_name: &str) {
        if let Some(handle) = self.0.write().await.remove(db_name) {
            tracing::info!(
                "Killing {} open streams from database \"{}\"",
                handle.connection_count(),
                db_name,
            );
        }
    }
}

impl Database {
    pub(crate) fn all_changes<'d>(
        &'d mut self,
    ) -> ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + 'd> {
        ChangesIter::new(move || {
            if !self.permissions().full() {
                return Err(CRRError::Unauthorized(
                    "Full access is required to listen to all changes".to_owned(),
                ));
            }

            let query = "
                SELECT \"table\", pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid())
                FROM crsql_changes
                WHERE db_version > ?
            ";

            let mut buffer = Vec::<Changeset>::new();
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;
                let authorized = self.disable_authorization();
                let mut stmt = authorized.prepare(query)?;
                let mut rows = stmt.query([&authorized.db_version()])?;

                while let Some(row) = rows.next()? {
                    let changeset: Changeset = row.try_into()?;
                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            if let Some(changeset) = buffer.last() {
                self.set_db_version(changeset.db_version());
            }

            Ok((buffer, has_next_page))
        })
    }
}
