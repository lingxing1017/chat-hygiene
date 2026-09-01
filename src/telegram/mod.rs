mod client;
mod connection_state;
mod contact_notifier;
mod models;
mod outbox;
mod parser;
mod webhook;
mod webhook_registration;

pub use client::{
    BusinessApi, DeleteAction, EditAction, ReadAction, SendAction, SentMessage, TelegramClient,
    TelegramError, WebhookApi, delete_message_batches,
};
pub use connection_state::{
    AuthenticatedBot, AuthoritativeBusinessConnection, AuthoritativeLookupError, BotIdentityApi,
    BoxFuture, BusinessConnectionApi, lookup_authenticated_bot,
    lookup_authenticated_bot_with_delay, lookup_business_connection,
    lookup_business_connection_with_delay,
};
pub use contact_notifier::{NewContactNotifierHandle, spawn_new_contact_notifier};
pub use models::{
    BusinessConnectionSnapshot, BusinessRights, OwnerCommandSnapshot, OwnerReplySnapshot,
    ParsedUpdate, RawBusinessEvent, RawEventKind,
};
pub use outbox::{DispatchError, DispatchOutcome, OutboxDispatcher};
pub use parser::{ParseError, parse_update, parse_update_with_owner_identity};
pub use webhook::{IngressError, WebhookInbox, webhook_router, webhook_router_with_owner_identity};
pub use webhook_registration::{
    WebhookFailureKind, WebhookFailureSummary, WebhookRegistrationError, reconcile_webhook,
};
