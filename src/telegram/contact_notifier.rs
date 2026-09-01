use crate::processing::{NewContactNotice, NewContactNotifier, NewContactNotifyError};

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

#[must_use]
pub fn spawn_new_contact_notifier<C>(client: C, capacity: usize) -> NewContactNotifierHandle
where
    C: BusinessApi + 'static,
{
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<NewContactNotice>(capacity.max(1));
    tokio::spawn(async move {
        while let Some(notice) = receiver.recv().await {
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
    });
    NewContactNotifierHandle { sender }
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
    use crate::processing::NewContactNotice;

    use super::{new_contact_action, new_contact_text};

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
