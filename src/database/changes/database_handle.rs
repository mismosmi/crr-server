use tokio::sync::{broadcast, mpsc};

use crate::error::CRRError;

use super::{Message, Signal};

pub(crate) type Subscription = broadcast::Receiver<Message>;

pub(crate) struct DatabaseHandle {
    message_sender: broadcast::Sender<Message>,
    signal_sender: mpsc::Sender<Signal>,
}

impl DatabaseHandle {
    pub(crate) fn from(
        message_sender: broadcast::Sender<Message>,
        signal_sender: mpsc::Sender<Signal>,
    ) -> Self {
        Self {
            message_sender,
            signal_sender,
        }
    }

    pub(crate) fn is_orphan(&self) -> bool {
        self.message_sender.receiver_count() < 1
    }

    pub(crate) async fn send_signal(&self, signal: Signal) -> Result<(), CRRError> {
        self.signal_sender.send(signal).await?;

        Ok(())
    }

    pub(crate) fn subscribe(&self) -> Subscription {
        self.message_sender.subscribe()
    }

    pub(crate) fn connection_count(&self) -> usize {
        self.message_sender.receiver_count()
    }
}
