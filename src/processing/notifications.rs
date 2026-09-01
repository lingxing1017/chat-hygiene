use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewContactNotice {
    pub owner_user_id: i64,
    pub owner_chat_id: i64,
    pub contact_chat_id: i64,
    pub username: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NewContactNotifyError {
    #[error("new-contact notification queue is full")]
    QueueFull,
    #[error("new-contact notification worker stopped")]
    WorkerStopped,
}

pub trait NewContactNotifier: Send + Sync {
    /// Queues a transient owner notification without waiting for delivery.
    ///
    /// # Errors
    ///
    /// Returns [`NewContactNotifyError`] when the bounded queue cannot accept
    /// the notification.
    fn try_notify(&self, notice: NewContactNotice) -> Result<(), NewContactNotifyError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopNewContactNotifier;

impl NewContactNotifier for NoopNewContactNotifier {
    fn try_notify(&self, _notice: NewContactNotice) -> Result<(), NewContactNotifyError> {
        Ok(())
    }
}
