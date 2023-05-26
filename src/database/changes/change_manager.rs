use std::{collections::hash_map::Entry, sync::Arc};

use tokio::sync::broadcast::{self, error::SendError};

use crate::{auth::DatabasePermissions, database::Database, error::CRRError};

use super::{DatabaseHandle, Message, Subscription};

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

    pub(crate) async fn subscribe(&self, db_name: &str) -> Result<Subscription, CRRError> {
        if let Some(handle) = self.0.read().await.get(db_name) {
            return Ok(handle.subscribe());
        }

        match self.0.write().await.entry(db_name.to_owned()) {
            Entry::Occupied(entry) => Ok(entry.get().subscribe()),
            Entry::Vacant(entry) => {
                let database =
                    Database::open_readonly_latest(db_name.to_owned(), DatabasePermissions::Full)?;
                let (handle, subscription) = Self::add_handle(database).await?;
                entry.insert(handle);

                Ok(subscription)
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn subscribe_for_test(
        &self,
        env: &crate::tests::TestEnv,
    ) -> Result<Subscription, CRRError> {
        tracing::info!("Subscribe for test");
        if let Some(handle) = self.0.read().await.get(env.db().name()) {
            return Ok(handle.subscribe());
        }

        match self
            .0
            .write()
            .await
            .entry(crate::tests::TestEnv::DB_NAME.to_owned())
        {
            Entry::Occupied(entry) => Ok(entry.get().subscribe()),
            Entry::Vacant(entry) => {
                let (handle, subscription) = Self::add_handle(env.db()).await?;
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
            move |_action, _arg1: &'_ str, _arg2: &'_ str, _rowid| {
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
