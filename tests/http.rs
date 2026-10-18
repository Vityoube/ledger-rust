use std::sync::Arc;

use ledger::http;
use ledger::ledger::Ledger;
use ureq::Agent;

struct Server {
    base: String,
    agent: Agent,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Server {
    fn start() -> Self {
        let (port_tx, port_rx) = std::sync::mpsc::channel();
        let (shutdown, stop) = tokio::sync::oneshot::channel::<()>();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                port_tx.send(listener.local_addr().unwrap().port()).unwrap();
                axum::serve(listener, http::app(Arc::new(Ledger::default())))
                    .with_graceful_shutdown(async {
                        let _ = stop.await;
                    })
                    .await
                    .unwrap();
            });
        });
        let port = port_rx.recv().unwrap();
        let agent: Agent = Agent::config_builder().http_status_as_error(false).build().into();
        Server { base: format!("http://127.0.0.1:{port}"), agent, shutdown: Some(shutdown) }
    }

    fn post(&self, path: &str, body: &str, correlation_id: Option<&str>) -> (u16, String) {
        let mut request = self.agent.post(format!("{}{path}", self.base)).header("Content-Type", "application/json");
        if let Some(id) = correlation_id {
            request = request.header("Correlation-Id", id);
        }
        let mut response = request.send(body).unwrap();
        (response.status().as_u16(), response.body_mut().read_to_string().unwrap())
    }

    fn get(&self, path: &str) -> (u16, String) {
        let mut response = self.agent.get(format!("{}{path}", self.base)).call().unwrap();
        (response.status().as_u16(), response.body_mut().read_to_string().unwrap())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

const ACCOUNTS: &str = "/accounts";
const TRANSFERS: &str = "/transfers";

#[test]
fn creates_reads_and_rejects_duplicate_accounts() {
    let server = Server::start();
    assert_eq!(201, server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":500}"#, None).0);
    assert_eq!(409, server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":900}"#, None).0);

    let (status, body) = server.get("/accounts/alice");
    assert_eq!(200, status);
    assert!(body.contains(r#""balance":500"#), "{body}");
    assert_eq!(404, server.get("/accounts/ghost").0);
}

#[test]
fn completed_transfer_is_replayed_on_retry_and_applied_once() {
    let server = Server::start();
    server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":100}"#, None);
    server.post(ACCOUNTS, r#"{"id":"bob","openingBalance":0}"#, None);
    let body = r#"{"from":"alice","to":"bob","amount":30}"#;

    let first = server.post(TRANSFERS, body, Some("key-1"));
    let retry = server.post(TRANSFERS, body, Some("key-1"));

    assert_eq!(201, first.0);
    assert_eq!(201, retry.0);
    assert_eq!(first.1, retry.1);
    assert!(server.get("/accounts/bob").1.contains(r#""balance":30"#));
}

#[test]
fn insufficient_funds_is_reported_as_unprocessable() {
    let server = Server::start();
    server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":10}"#, None);
    server.post(ACCOUNTS, r#"{"id":"bob","openingBalance":0}"#, None);

    let (status, body) = server.post(TRANSFERS, r#"{"from":"alice","to":"bob","amount":30}"#, Some("k"));

    assert_eq!(422, status);
    assert!(body.contains("INSUFFICIENT_FUNDS"), "{body}");
}

#[test]
fn reusing_key_for_different_body_is_conflict() {
    let server = Server::start();
    server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":100}"#, None);
    server.post(ACCOUNTS, r#"{"id":"bob","openingBalance":0}"#, None);
    server.post(ACCOUNTS, r#"{"id":"carol","openingBalance":0}"#, None);
    server.post(TRANSFERS, r#"{"from":"alice","to":"bob","amount":10}"#, Some("k"));

    let (status, _) = server.post(TRANSFERS, r#"{"from":"alice","to":"carol","amount":10}"#, Some("k"));

    assert_eq!(409, status);
}

#[test]
fn bad_input_is_bad_request() {
    let server = Server::start();
    server.post(ACCOUNTS, r#"{"id":"alice","openingBalance":100}"#, None);
    server.post(ACCOUNTS, r#"{"id":"bob","openingBalance":0}"#, None);

    assert_eq!(400, server.post(TRANSFERS, r#"{"from":"alice","to":"bob","amount":10}"#, None).0);
    assert_eq!(400, server.post(TRANSFERS, "not json", Some("k1")).0);
    assert_eq!(400, server.post(TRANSFERS, r#"{"from":"alice","to":"alice","amount":10}"#, Some("k2")).0);
    assert_eq!(400, server.post(TRANSFERS, r#"{"from":"alice","to":"bob","amount":1.5}"#, Some("k3")).0);
    assert_eq!(400, server.post(ACCOUNTS, r#"{"id":"carol","openingBalance":-1}"#, None).0);
    assert_eq!(404, server.get("/nowhere").0);
}
