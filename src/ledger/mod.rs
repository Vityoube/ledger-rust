//! The ledger core: plain Rust with no web framework dependencies.

mod store;

#[cfg(test)]
mod concurrency_tests;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

pub use store::{InMemoryLedgerStore, LedgerStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    pub id: String,
    pub balance: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Completed,
    Rejected,
}

impl Status {
    pub fn name(self) -> &'static str {
        match self {
            Status::Completed => "COMPLETED",
            Status::Rejected => "REJECTED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    UnknownAccount,
    InsufficientFunds,
}

impl RejectionReason {
    pub fn name(self) -> &'static str {
        match self {
            RejectionReason::UnknownAccount => "UNKNOWN_ACCOUNT",
            RejectionReason::InsufficientFunds => "INSUFFICIENT_FUNDS",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRequest {
    from: String,
    to: String,
    amount: i64,
}

impl TransferRequest {
    pub fn new(from: &str, to: &str, amount: i64) -> Result<Self, LedgerError> {
        if from.trim().is_empty() {
            return Err(LedgerError::invalid("from is required"));
        }
        if to.trim().is_empty() {
            return Err(LedgerError::invalid("to is required"));
        }
        if from == to {
            return Err(LedgerError::invalid("from and to must differ"));
        }
        if amount <= 0 {
            return Err(LedgerError::invalid("amount must be positive"));
        }
        Ok(Self { from: from.to_owned(), to: to.to_owned(), amount })
    }

    pub fn from(&self) -> &str {
        &self.from
    }

    pub fn to(&self) -> &str {
        &self.to
    }

    pub fn amount(&self) -> i64 {
        self.amount
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOutcome {
    pub transfer_id: u64,
    pub request: TransferRequest,
    pub status: Status,
    pub reason: Option<RejectionReason>,
}

impl TransferOutcome {
    fn completed(transfer_id: u64, request: &TransferRequest) -> Self {
        Self { transfer_id, request: request.clone(), status: Status::Completed, reason: None }
    }

    fn rejected(transfer_id: u64, request: &TransferRequest, reason: RejectionReason) -> Self {
        Self { transfer_id, request: request.clone(), status: Status::Rejected, reason: Some(reason) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LedgerError {
    /// Bad input from the caller.
    InvalidInput(String),
    /// A correlation id was reused for a different transfer.
    IdempotencyConflict(String),
    /// The store failed (for example an arithmetic overflow).
    Internal(String),
}

impl LedgerError {
    fn invalid(message: &str) -> Self {
        LedgerError::InvalidInput(message.to_owned())
    }
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::InvalidInput(m) | LedgerError::IdempotencyConflict(m) | LedgerError::Internal(m) => {
                f.write_str(m)
            }
        }
    }
}

impl std::error::Error for LedgerError {}

pub struct Ledger {
    store: Box<dyn LedgerStore>,
    next_transfer_id: AtomicU64,
    pending: Mutex<HashMap<String, Arc<Pending>>>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new(Box::new(InMemoryLedgerStore::new()))
    }
}

impl Ledger {
    pub fn new(store: Box<dyn LedgerStore>) -> Self {
        Self { store, next_transfer_id: AtomicU64::new(1), pending: Mutex::new(HashMap::new()) }
    }

    /// Returns `false` if the account already exists.
    pub fn create_account(&self, id: &str, opening_balance: i64) -> Result<bool, LedgerError> {
        require_text(id, "id")?;
        if opening_balance < 0 {
            return Err(LedgerError::invalid("openingBalance must not be negative"));
        }
        Ok(self.store.create_account(id, opening_balance))
    }

    pub fn account(&self, id: &str) -> Result<Option<Account>, LedgerError> {
        require_text(id, "id")?;
        Ok(self.store.find_account(id))
    }

    pub fn transfer(&self, correlation_id: &str, request: TransferRequest) -> Result<TransferOutcome, LedgerError> {
        require_text(correlation_id, "correlationId")?;

        let claim = Arc::new(Pending::new(request.clone()));
        let existing = {
            let mut pending = lock(&self.pending);
            match pending.get(correlation_id) {
                Some(existing) => Some(Arc::clone(existing)),
                None => {
                    pending.insert(correlation_id.to_owned(), Arc::clone(&claim));
                    None
                }
            }
        };
        if let Some(existing) = existing {
            return existing.wait(correlation_id, &request);
        }

        let transfer_id = self.next_transfer_id.fetch_add(1, Ordering::Relaxed);
        let result = self.store.apply_transfer(transfer_id, &request);
        if result.is_err() {
            // A failed attempt did not happen, so the key can be retried.
            lock(&self.pending).remove(correlation_id);
        }
        claim.complete(result.clone());
        result
    }
}

fn require_text(value: &str, name: &str) -> Result<(), LedgerError> {
    if value.trim().is_empty() {
        return Err(LedgerError::InvalidInput(format!("{name} is required")));
    }
    Ok(())
}

/// A poisoned lock only means another thread panicked; the guarded data is still consistent here.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The first request for a correlation id; retries wait on its outcome.
struct Pending {
    request: TransferRequest,
    outcome: Mutex<Option<Result<TransferOutcome, LedgerError>>>,
    done: Condvar,
}

impl Pending {
    fn new(request: TransferRequest) -> Self {
        Self { request, outcome: Mutex::new(None), done: Condvar::new() }
    }

    fn complete(&self, result: Result<TransferOutcome, LedgerError>) {
        *lock(&self.outcome) = Some(result);
        self.done.notify_all();
    }

    fn wait(&self, correlation_id: &str, retry: &TransferRequest) -> Result<TransferOutcome, LedgerError> {
        if &self.request != retry {
            return Err(LedgerError::IdempotencyConflict(format!(
                "correlation id {correlation_id} was already used for a different transfer"
            )));
        }
        let mut outcome = lock(&self.outcome);
        loop {
            if let Some(result) = outcome.as_ref() {
                return result.clone();
            }
            outcome = self.done.wait(outcome).unwrap_or_else(|e| e.into_inner());
        }
    }
}
