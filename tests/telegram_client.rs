use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::extract::{OriginalUri, State};
use axum::http::StatusCode;
use axum::routing::any;
use axum::{Json, Router};
use chathygiene::telegram::{
    BotIdentityApi, BusinessApi, BusinessConnectionApi, DeleteAction, EditAction, ReadAction,
    SendAction, TelegramClient, TelegramError, delete_message_batches,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use url::Url;

#[derive(Clone)]
struct StubState {
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    responses: Arc<Mutex<VecDeque<(StatusCode, Value)>>>,
}

async fn capture(
    State(state): State<StubState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state
        .requests
        .lock()
        .unwrap()
        .push((uri.path().to_owned(), body));
    let (status, response) = state.responses.lock().unwrap().pop_front().unwrap();
    (status, Json(response))
}

async fn stub(responses: Vec<(StatusCode, Value)>) -> (Url, Arc<Mutex<Vec<(String, Value)>>>) {
    let state = StubState {
        requests: Arc::new(Mutex::new(Vec::new())),
        responses: Arc::new(Mutex::new(responses.into())),
    };
    let requests = Arc::clone(&state.requests);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(any(capture)).with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (Url::parse(&format!("http://{address}/")).unwrap(), requests)
}

#[tokio::test]
async fn client_sends_exact_business_payloads_without_exposing_token() {
    let (base_url, requests) = stub(vec![
        (
            StatusCode::OK,
            json!({"ok": true, "result": {"message_id": 901}}),
        ),
        (StatusCode::OK, json!({"ok": true, "result": true})),
        (StatusCode::OK, json!({"ok": true, "result": true})),
        (StatusCode::OK, json!({"ok": true, "result": true})),
    ])
    .await;
    let token = "123456:super-secret";
    let client = TelegramClient::with_base_url(
        reqwest::Client::new(),
        SecretString::from(token.to_owned()),
        base_url,
    );

    let sent = client
        .send_business_message(&SendAction {
            business_connection_id: Some("business-1".to_owned()),
            chat_id: 1001,
            text: "7 + 5 - 3 = ?".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(sent.message_id, 901);
    client
        .edit_business_message(&EditAction {
            business_connection_id: "business-1".to_owned(),
            chat_id: 1001,
            message_id: 901,
            text: "验证成功".to_owned(),
        })
        .await
        .unwrap();
    client
        .read_business_message(&ReadAction {
            business_connection_id: "business-1".to_owned(),
            chat_id: 1001,
            message_id: 88,
        })
        .await
        .unwrap();
    client
        .delete_business_messages(&DeleteAction {
            business_connection_id: "business-1".to_owned(),
            message_ids: vec![88, 89],
        })
        .await
        .unwrap();

    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].0, format!("/bot{token}/sendMessage"));
    assert_eq!(
        requests[0].1,
        json!({
            "business_connection_id": "business-1",
            "chat_id": 1001,
            "text": "7 + 5 - 3 = ?"
        })
    );
    assert_eq!(requests[1].0, format!("/bot{token}/editMessageText"));
    assert_eq!(
        requests[1].1,
        json!({
            "business_connection_id": "business-1",
            "chat_id": 1001,
            "message_id": 901,
            "text": "验证成功"
        })
    );
    assert_eq!(requests[2].0, format!("/bot{token}/readBusinessMessage"));
    assert_eq!(
        requests[2].1,
        json!({
            "business_connection_id": "business-1",
            "chat_id": 1001,
            "message_id": 88
        })
    );
    assert_eq!(requests[3].0, format!("/bot{token}/deleteBusinessMessages"));
    assert_eq!(
        requests[3].1,
        json!({
            "business_connection_id": "business-1",
            "message_ids": [88, 89]
        })
    );
    assert!(!format!("{client:?}").contains(token));
}

#[tokio::test]
async fn client_parses_retry_after_and_rejects_invalid_delete_batches() {
    let (base_url, _) = stub(vec![(
        StatusCode::TOO_MANY_REQUESTS,
        json!({
            "ok": false,
            "error_code": 429,
            "description": "Too Many Requests",
            "parameters": {"retry_after": 93}
        }),
    )])
    .await;
    let client = TelegramClient::with_base_url(
        reqwest::Client::new(),
        SecretString::from("token".to_owned()),
        base_url,
    );
    let error = client
        .read_business_message(&ReadAction {
            business_connection_id: "business-1".to_owned(),
            chat_id: 1001,
            message_id: 88,
        })
        .await
        .unwrap_err();
    assert_eq!(
        error,
        TelegramError::Api {
            error_code: 429,
            description: "Too Many Requests".to_owned(),
            retry_after: Some(93),
        }
    );

    for message_ids in [Vec::new(), (1..=101).collect()] {
        let error = client
            .delete_business_messages(&DeleteAction {
                business_connection_id: "business-1".to_owned(),
                message_ids,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, TelegramError::InvalidRequest(_)));
    }
}

#[tokio::test]
async fn client_queries_exact_authoritative_endpoints_and_discards_profile_fields() {
    let (base_url, requests) = stub(vec![
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": 123_456,
                    "is_bot": true,
                    "first_name": "must be discarded",
                    "username": "must_not_escape"
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "business-full",
                    "user": {"id": 42, "first_name": "discarded"},
                    "user_chat_id": 4200,
                    "date": 1_789_000_000,
                    "rights": {
                        "can_reply": true,
                        "can_read_messages": true,
                        "can_delete_sent_messages": true,
                        "can_delete_all_messages": true
                    },
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "business-limited",
                    "user": {"id": 42},
                    "date": 1_789_000_001,
                    "rights": {
                        "can_reply": false,
                        "can_read_messages": true,
                        "can_delete_sent_messages": false,
                        "can_delete_all_messages": false
                    },
                    "is_enabled": false
                }
            }),
        ),
    ])
    .await;
    let token = "123456:authoritative-secret";
    let client = TelegramClient::with_base_url(
        reqwest::Client::new(),
        SecretString::from(token.to_owned()),
        base_url,
    );

    assert_eq!(client.get_me().await.unwrap().id, 123_456);
    let full = client
        .get_business_connection("business-full")
        .await
        .unwrap();
    assert_eq!(full.connection_id, "business-full");
    assert_eq!(full.business_user_id, 42);
    assert_eq!(full.user_chat_id, Some(4200));
    assert_eq!(full.connection_established_at, 1_789_000_000);
    assert!(full.enabled);
    assert!(full.rights.can_reply);
    assert!(full.rights.can_read_messages);
    assert!(full.rights.can_delete_sent_messages);
    assert!(full.rights.can_delete_all_messages);
    let limited = client
        .get_business_connection("business-limited")
        .await
        .unwrap();
    assert_eq!(limited.user_chat_id, None);
    assert!(!limited.enabled);
    assert!(!limited.rights.can_reply);
    assert!(limited.rights.can_read_messages);
    assert!(!limited.rights.can_delete_sent_messages);
    assert!(!limited.rights.can_delete_all_messages);

    let requests = requests.lock().unwrap();
    assert_eq!(requests[0].0, format!("/bot{token}/getMe"));
    assert_eq!(requests[0].1, json!({}));
    assert_eq!(
        requests[1],
        (
            format!("/bot{token}/getBusinessConnection"),
            json!({"business_connection_id": "business-full"})
        )
    );
    assert_eq!(
        requests[2],
        (
            format!("/bot{token}/getBusinessConnection"),
            json!({"business_connection_id": "business-limited"})
        )
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn client_strictly_rejects_malformed_authoritative_successes() {
    let valid_rights = json!({
        "can_reply": true,
        "can_read_messages": true,
        "can_delete_sent_messages": true,
        "can_delete_all_messages": true
    });
    let responses = vec![
        (StatusCode::OK, json!({"ok": true})),
        (
            StatusCode::OK,
            json!({"ok": true, "result": {"id": 1, "is_bot": false}}),
        ),
        (
            StatusCode::OK,
            json!({"ok": true, "result": {"id": "1", "is_bot": true}}),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "different-id",
                    "user": {"id": 42},
                    "user_chat_id": 4200,
                    "date": 100,
                    "rights": valid_rights,
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "expected-id",
                    "user": {"id": 0},
                    "user_chat_id": 4200,
                    "date": 100,
                    "rights": valid_rights,
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "expected-id",
                    "user": {"id": 42},
                    "user_chat_id": 0,
                    "date": 100,
                    "rights": valid_rights,
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "expected-id",
                    "user": {"id": 42},
                    "date": "100",
                    "rights": valid_rights,
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "expected-id",
                    "user": {"id": 42},
                    "date": 100,
                    "rights": {
                        "can_reply": true,
                        "can_read_messages": true,
                        "can_delete_sent_messages": true
                    },
                    "is_enabled": true
                }
            }),
        ),
        (
            StatusCode::OK,
            json!({
                "ok": true,
                "result": {
                    "id": "expected-id",
                    "user": {"id": 42},
                    "date": 100,
                    "rights": {
                        "can_reply": true,
                        "can_read_messages": true,
                        "can_delete_sent_messages": true,
                        "can_delete_all_messages": true,
                        "unexpected": true
                    },
                    "is_enabled": true
                }
            }),
        ),
        (StatusCode::OK, json!({"ok": true})),
    ];
    let (base_url, _) = stub(responses).await;
    let client = TelegramClient::with_base_url(
        reqwest::Client::new(),
        SecretString::from("strict-token".to_owned()),
        base_url,
    );

    for result in [
        client.get_me().await,
        client.get_me().await,
        client.get_me().await,
    ] {
        assert!(matches!(result.unwrap_err(), TelegramError::Protocol(_)));
    }
    for _ in 0..7 {
        assert!(matches!(
            client
                .get_business_connection("expected-id")
                .await
                .unwrap_err(),
            TelegramError::Protocol(_)
        ));
    }
    assert!(matches!(
        client.get_business_connection(" ").await.unwrap_err(),
        TelegramError::InvalidRequest(_)
    ));
}

#[tokio::test]
async fn authoritative_client_errors_redact_request_and_response_values() {
    let token = "123456:sentinel-token";
    let connection_id = "sentinel-connection-id";
    let description = "sentinel response description";
    let (base_url, _) = stub(vec![(
        StatusCode::BAD_REQUEST,
        json!({
            "ok": false,
            "error_code": 400,
            "description": description,
            "sentinel_body": "sentinel response body"
        }),
    )])
    .await;
    let base_url_text = base_url.to_string();
    let client = TelegramClient::with_base_url(
        reqwest::Client::new(),
        SecretString::from(token.to_owned()),
        base_url,
    );
    let error = client
        .get_business_connection(connection_id)
        .await
        .unwrap_err();
    let rendered = format!("{error:?} {error} {client:?}");
    for secret in [
        token,
        connection_id,
        description,
        "sentinel response body",
        &base_url_text,
    ] {
        assert!(!rendered.contains(secret));
    }
}

#[test]
fn deletion_batches_cover_telegram_limits_without_loss() {
    for (count, expected_lengths) in [(101, vec![100, 1]), (250, vec![100, 100, 50])] {
        let ids = (1..=count).collect::<Vec<_>>();
        let batches = delete_message_batches(&ids);
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            expected_lengths
        );
        assert_eq!(batches.concat(), ids);
    }
}
