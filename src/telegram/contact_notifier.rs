use crate::processing::{NewContactNotice, NewContactNotifier, NewContactNotifyError};
use crate::runtime_workers::WorkerTask;

use super::client::{BusinessApi, SendAction};

#[derive(Debug, Clone)]
pub struct NewContactNotifierHandle {
    sender: tokio::sync::mpsc::Sender<NewContactNotice>,
}

impl NewContactNotifier for NewContactNotifierHandle {
    fn try_notify(&self, notice: NewContactNotice) -> Result<(), NewContactNotifyError> {
        self.sender.try_send(notice).map_err(|error| match error {
            tokio::sync::mpsc::error::TrySendError::Full(_) => NewContactNotifyError::QueueFull,
            tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                NewContactNotifyError::WorkerStopped
            }
        })
    }
}

#[must_use = "the notifier worker must remain owned until it is shut down"]
pub struct NewContactNotifierWorker {
    handle: NewContactNotifierHandle,
    task: Option<WorkerTask>,
}

impl NewContactNotifierWorker {
    #[must_use]
    pub fn handle(&self) -> NewContactNotifierHandle {
        self.handle.clone()
    }

    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take()
            && task.stop_and_wait().await.is_err()
        {
            tracing::warn!(
                error_kind = "worker_shutdown",
                "new-contact notifier shutdown failed"
            );
        }
    }

    pub(crate) fn into_task(mut self) -> WorkerTask {
        self.task.take().expect("notifier worker task is owned")
    }
}

pub fn spawn_new_contact_notifier<C>(client: C, capacity: usize) -> NewContactNotifierWorker
where
    C: BusinessApi + 'static,
{
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<NewContactNotice>(capacity.max(1));
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                result = stopped.changed() => {
                    if result.is_err() || *stopped.borrow() {
                        break;
                    }
                }
                notice = receiver.recv() => {
                    let Some(notice) = notice else {
                        break;
                    };
                    let owner_user_id = notice.owner_user_id;
                    let contact_chat_id = notice.contact_chat_id;
                    let action = new_contact_action(&notice);
                    if client.send_business_message(&action).await.is_err() {
                        tracing::warn!(
                            owner_user_id,
                            contact_chat_id,
                            "new-contact notification delivery failed"
                        );
                    }
                }
            }
        }
    });
    NewContactNotifierWorker {
        handle: NewContactNotifierHandle { sender },
        task: Some(WorkerTask::new(stop, task)),
    }
}

fn new_contact_action(notice: &NewContactNotice) -> SendAction {
    SendAction {
        business_connection_id: None,
        chat_id: notice.owner_chat_id,
        text: new_contact_text(notice),
    }
}

fn new_contact_text(notice: &NewContactNotice) -> String {
    let contact = notice.username.as_ref().map_or_else(
        || "未设置用户名".to_owned(),
        |username| format!("@{username}"),
    );
    format!(
        "新联系人：{contact}\n用户 ID：{}\n状态：等待验证\n\n/inspect {}\n/reset {}\n/unblock {}",
        notice.contact_chat_id,
        notice.contact_chat_id,
        notice.contact_chat_id,
        notice.contact_chat_id
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::processing::{NewContactNotice, NewContactNotifier, NewContactNotifyError};

    use super::super::client::{
        BusinessApi, DeleteAction, EditAction, ReadAction, SendAction, SentMessage, TelegramError,
    };
    use super::{new_contact_action, new_contact_text, spawn_new_contact_notifier};

    #[derive(Clone, Default)]
    struct RecordingClient {
        sends: Arc<AtomicUsize>,
    }

    impl BusinessApi for RecordingClient {
        fn send_business_message<'a>(
            &'a self,
            _action: &'a SendAction,
        ) -> Pin<Box<dyn Future<Output = Result<SentMessage, TelegramError>> + Send + 'a>> {
            Box::pin(async move {
                self.sends.fetch_add(1, Ordering::SeqCst);
                Ok(SentMessage { message_id: 1 })
            })
        }

        fn edit_business_message<'a>(
            &'a self,
            _action: &'a EditAction,
        ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
            Box::pin(async { unreachable!("notifier never edits") })
        }

        fn read_business_message<'a>(
            &'a self,
            _action: &'a ReadAction,
        ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
            Box::pin(async { unreachable!("notifier never reads") })
        }

        fn delete_business_messages<'a>(
            &'a self,
            _action: &'a DeleteAction,
        ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
            Box::pin(async { unreachable!("notifier never deletes") })
        }
    }

    #[tokio::test]
    async fn owned_notifier_delivers_then_closes_on_shutdown() {
        let client = RecordingClient::default();
        let worker = spawn_new_contact_notifier(client.clone(), 1);
        let handle = worker.handle();
        handle
            .try_notify(NewContactNotice {
                owner_user_id: 42,
                owner_chat_id: 4200,
                contact_chat_id: 1001,
                username: None,
            })
            .unwrap();
        for _ in 0..20 {
            if client.sends.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(client.sends.load(Ordering::SeqCst), 1);

        worker.shutdown().await;

        assert_eq!(
            handle
                .try_notify(NewContactNotice {
                    owner_user_id: 42,
                    owner_chat_id: 4200,
                    contact_chat_id: 1002,
                    username: None,
                })
                .unwrap_err(),
            NewContactNotifyError::WorkerStopped
        );
    }

    #[test]
    fn routes_notification_to_owner_chat_without_business_connection() {
        let action = new_contact_action(&NewContactNotice {
            owner_user_id: 42,
            owner_chat_id: 4200,
            contact_chat_id: 1001,
            username: Some("sample_user".to_owned()),
        });

        assert_eq!(action.business_connection_id, None);
        assert_eq!(action.chat_id, 4200);
    }

    #[test]
    fn renders_username_and_numeric_owner_commands() {
        assert_eq!(
            new_contact_text(&NewContactNotice {
                owner_user_id: 42,
                owner_chat_id: 4200,
                contact_chat_id: 1001,
                username: Some("sample_user".to_owned()),
            }),
            "新联系人：@sample_user\n用户 ID：1001\n状态：等待验证\n\n/inspect 1001\n/reset 1001\n/unblock 1001"
        );
    }

    #[test]
    fn renders_missing_username_without_changing_command_ids() {
        assert_eq!(
            new_contact_text(&NewContactNotice {
                owner_user_id: 42,
                owner_chat_id: 4200,
                contact_chat_id: 1001,
                username: None,
            }),
            "新联系人：未设置用户名\n用户 ID：1001\n状态：等待验证\n\n/inspect 1001\n/reset 1001\n/unblock 1001"
        );
    }
}
