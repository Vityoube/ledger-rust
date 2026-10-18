//! A thin JSON adapter over the ledger. It contains no money logic.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::ledger::{Account, Ledger, LedgerError, Status, TransferOutcome, TransferRequest};

const ACCOUNTS_PATH: &str = "/accounts";
const ACCOUNT_PATH_PREFIX: &str = "/accounts/";
const TRANSFERS_PATH: &str = "/transfers";
const CORRELATION_ID_HEADER: &str = "Correlation-Id";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CreateAccountBody {
    id: Option<String>,
    opening_balance: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferBody {
    from: Option<String>,
    to: Option<String>,
    amount: Option<i64>,
}

#[derive(Serialize)]
struct AccountView {
    id: String,
    balance: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TransferView {
    transfer_id: u64,
    status: &'static str,
    reason: Option<&'static str>,
    from: String,
    to: String,
    amount: i64,
}

impl From<Account> for AccountView {
    fn from(account: Account) -> Self {
        Self { id: account.id, balance: account.balance }
    }
}

impl From<&TransferOutcome> for TransferView {
    fn from(outcome: &TransferOutcome) -> Self {
        Self {
            transfer_id: outcome.transfer_id,
            status: outcome.status.name(),
            reason: outcome.reason.map(|r| r.name()),
            from: outcome.request.from().to_owned(),
            to: outcome.request.to().to_owned(),
            amount: outcome.request.amount(),
        }
    }
}

struct Reply {
    status: StatusCode,
    body: serde_json::Value,
}

impl Reply {
    fn new(status: StatusCode, body: impl Serialize) -> Self {
        Self { status, body: serde_json::to_value(body).expect("response is serializable") }
    }

    fn error(status: StatusCode, message: &str) -> Self {
        Self { status, body: json!({ "error": message }) }
    }
}

impl From<LedgerError> for Reply {
    fn from(error: LedgerError) -> Self {
        match error {
            LedgerError::InvalidInput(m) => Reply::error(StatusCode::BAD_REQUEST, &m),
            LedgerError::IdempotencyConflict(m) => Reply::error(StatusCode::CONFLICT, &m),
            LedgerError::Internal(_) => Reply::error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"),
        }
    }
}

impl IntoResponse for Reply {
    fn into_response(self) -> Response {
        let bytes = serde_json::to_vec(&self.body).expect("JSON value is serializable");
        (self.status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
    }
}

/// Builds the HTTP application. Every request goes through one handler, as the routes are few and fixed.
pub fn app(ledger: Arc<Ledger>) -> Router {
    Router::new().fallback(move |method: Method, uri: Uri, headers: HeaderMap, body: Bytes| {
        let ledger = Arc::clone(&ledger);
        async move {
            // A retry can block waiting for the original transfer, so keep it off the async workers.
            let reply = tokio::task::spawn_blocking(move || route(&ledger, &method, &uri, &headers, &body))
                .await
                .unwrap_or_else(|_| Reply::error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"));
            reply.into_response()
        }
    })
}

fn route(ledger: &Ledger, method: &Method, uri: &Uri, headers: &HeaderMap, body: &[u8]) -> Reply {
    let result = (|| {
        let path = uri.path();
        if path == ACCOUNTS_PATH && method == Method::POST {
            return create_account(ledger, read_body(body)?);
        }
        if let Some(id) = path.strip_prefix(ACCOUNT_PATH_PREFIX) {
            if method == Method::GET {
                return get_account(ledger, &percent_decode(id));
            }
        }
        if path == TRANSFERS_PATH && method == Method::POST {
            return transfer(ledger, headers, body);
        }
        Ok(Reply::error(StatusCode::NOT_FOUND, "no such endpoint"))
    })();
    result.unwrap_or_else(Reply::from)
}

fn create_account(ledger: &Ledger, body: Option<CreateAccountBody>) -> Result<Reply, LedgerError> {
    let Some(CreateAccountBody { id: Some(id), opening_balance }) = body else {
        return Err(LedgerError::InvalidInput("id is required".to_owned()));
    };
    let opening_balance = opening_balance.unwrap_or(0);
    if !ledger.create_account(&id, opening_balance)? {
        return Ok(Reply::error(StatusCode::CONFLICT, "account already exists"));
    }
    Ok(Reply::new(StatusCode::CREATED, AccountView { id, balance: opening_balance }))
}

fn get_account(ledger: &Ledger, id: &str) -> Result<Reply, LedgerError> {
    Ok(match ledger.account(id)? {
        Some(account) => Reply::new(StatusCode::OK, AccountView::from(account)),
        None => Reply::error(StatusCode::NOT_FOUND, "no such account"),
    })
}

fn transfer(ledger: &Ledger, headers: &HeaderMap, body: &[u8]) -> Result<Reply, LedgerError> {
    let correlation_id = headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| LedgerError::InvalidInput(format!("{CORRELATION_ID_HEADER} header is required")))?;
    let Some(TransferBody { from: Some(from), to: Some(to), amount: Some(amount) }) = read_body(body)? else {
        return Err(LedgerError::InvalidInput("from, to and amount are required".to_owned()));
    };
    let outcome = ledger.transfer(correlation_id, TransferRequest::new(&from, &to, amount)?)?;
    let status = match outcome.status {
        Status::Completed => StatusCode::CREATED,
        Status::Rejected => StatusCode::UNPROCESSABLE_ENTITY,
    };
    Ok(Reply::new(status, TransferView::from(&outcome)))
}

/// A JSON `null` body is `None`, like any body that lacks the required fields.
fn read_body<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<Option<T>, LedgerError> {
    serde_json::from_slice(body).map_err(|e| LedgerError::InvalidInput(format!("invalid JSON body: {e}")))
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        if bytes[i] == b'%' && i + 2 < bytes.len() && let (Some(h), Some(l)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
            out.push((h * 16 + l) as u8);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
