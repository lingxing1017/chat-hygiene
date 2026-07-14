use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::extract::{OriginalUri, State};
use axum::http::StatusCode;
use axum::routing::any;
use axum::{Json, Router};
use chathygiene::telegram::{
    BusinessApi, DeleteAction, EditAction, ReadAction, SendAction, TelegramClient, TelegramError,
    delete_message_batches,
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
