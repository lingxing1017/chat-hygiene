mod database;
mod key_material;
mod models;
mod owner_identity;
mod repositories;
mod uow;

pub use database::{StorageError, connect, migrate};
pub use key_material::{MasterSeed, load_or_initialize_master_seed, pin_or_verify_telegram_bot_id};
pub use models::{
    BusinessConnectionRecord, ChallengeHmacUpgradeRecord, ChallengeRecord, Conversation,
    ConversationKey, LedgerMessage, MessageDirection, NewAuditEvent, NewOutboxAction,
    OutboxActionKind, OutboxActionRecord, OwnerChatSource, OwnerIdentity, SenderKind,
};
pub use owner_identity::{
    advance_owner_connection_floor, claim_owner, initialize_or_load_owner_identity,
    load_owner_identity, promote_owner_chat,
};
pub use repositories::{
    active_challenge, active_challenges_not_on_version, active_owner_reply_ids,
    claim_due_outbox_action, close_active_challenge, close_challenge, create_challenge,
    disable_business_connection, eligible_deletion_ids, enqueue_outbox_action,
    find_business_connection, find_challenge_by_id, find_conversation,
    find_single_business_connection, get_or_create_conversation, increment_challenge_attempts,
    insert_audit_event, list_outbox_actions_for_update, mark_challenge_sent,
    mark_challenge_uncertain, mark_message_deleted, mark_outbox_permanent_failure,
    mark_outbox_retry, mark_outbox_succeeded, mark_outbox_uncertain, record_message,
    replace_challenge_hmac, save_conversation, upsert_business_connection,
};
pub use uow::UnitOfWork;
