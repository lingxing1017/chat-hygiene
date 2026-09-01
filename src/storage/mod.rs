mod connection_candidate;
mod database;
mod key_material;
mod models;
mod owner_identity;
mod repositories;
mod uow;

pub use connection_candidate::{
    BusinessConnectionCandidate, CandidateGuard, CandidateWrite, ConnectionReconciliationSnapshot,
    GlobalReconciliationState, TelegramReconciliationState, TrustedConnectionWrite,
    apply_authoritative_candidate, candidates_for_user, clear_connection_candidates,
    connection_reconciliation_snapshot, delete_connection_candidate,
    delete_connection_candidates_for_other_users, gate_matching_trusted_for_reconciliation,
    load_candidate_guard, load_telegram_reconciliation_state, normalize_startup_reconciliation,
    owner_generation_floor, promote_claim_candidate, prune_connection_candidates,
    reconcile_authoritative_trusted_connection, retain_connection_candidates,
    retire_trusted_connection_not_found, set_telegram_auth_failed,
    transition_telegram_reconciliation_ready,
};
pub(crate) use database::ServiceDatabaseDescriptor;
pub use database::{StorageError, connect, migrate};
pub use key_material::{MasterSeed, load_or_initialize_master_seed, pin_or_verify_telegram_bot_id};
pub use models::{
    BusinessConnectionRecord, ChallengeHmacUpgradeRecord, ChallengeRecord, Conversation,
    ConversationKey, LedgerMessage, MessageDirection, NewAuditEvent, NewOutboxAction,
    OutboxActionKind, OutboxActionRecord, OwnerChatSource, OwnerIdentity, ReconciliationState,
    SenderKind,
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
    insert_audit_event, list_outbox_actions_for_update,
    load_business_connection_for_reconciliation, load_single_trusted_connection,
    mark_challenge_sent, mark_challenge_uncertain, mark_message_deleted,
    mark_outbox_permanent_failure, mark_outbox_retry, mark_outbox_succeeded, mark_outbox_uncertain,
    record_message, replace_challenge_hmac, save_conversation, upsert_business_connection,
};
pub use uow::UnitOfWork;
