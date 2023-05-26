use tokio::sync::{broadcast, mpsc};

use crate::error::CRRError;

use super::Message;

pub(crate) type Subscription = broadcast::Receiver<Message>;

pub(crate) struct DatabaseHandle {
    message_sender: broadcast::Sender<Message>,
    signal_sender: mpsc::Sender<()>,
}

impl DatabaseHandle {
    pub(crate) fn from(
        message_sender: broadcast::Sender<Message>,
        signal_sender: mpsc::Sender<()>,
    ) -> Self {
        Self {
            message_sender,
            signal_sender,
        }
    }

    pub(crate) fn is_orphan(&self) -> bool {
        self.message_sender.receiver_count() < 1
    }

    pub(crate) async fn send_signal(&self) -> Result<(), CRRError> {
        self.signal_sender.send(()).await?;

        Ok(())
    }

    pub(crate) fn subscribe(&self) -> Subscription {
        self.message_sender.subscribe()
    }

    pub(crate) fn connection_count(&self) -> usize {
        self.message_sender.receiver_count()
    }

    pub(crate) fn release(self) -> broadcast::Sender<Message> {
        self.message_sender
    }
}
