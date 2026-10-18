use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use super::{Account, LedgerError, RejectionReason, TransferOutcome, TransferRequest};

/// The storage port. Implementations must make `apply_transfer` atomic.
pub trait LedgerStore: Send + Sync {
    /// Returns `false` if the account already exists.
    fn create_account(&self, id: &str, opening_balance: i64) -> bool;

    fn find_account(&self, id: &str) -> Option<Account>;

    fn apply_transfer(&self, transfer_id: u64, request: &TransferRequest) -> Result<TransferOutcome, LedgerError>;
}

pub(super) const OPENING_TRANSFER_ID: u64 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JournalEntry {
    pub transfer_id: u64,
    pub delta: i64,
    pub balance_after: i64,
}

struct AccountCell {
    id: String,
    state: Mutex<CellState>,
}

#[derive(Default)]
struct CellState {
    balance: i64,
    journal: Vec<JournalEntry>,
}

impl CellState {
    fn post(&mut self, transfer_id: u64, delta: i64) {
        self.balance += delta;
        self.journal.push(JournalEntry { transfer_id, delta, balance_after: self.balance });
    }
}

type FirstLockHook = Box<dyn Fn(&str) + Send + Sync>;

/// In-memory store with one lock per account; a transfer locks its two accounts in id order.
pub struct InMemoryLedgerStore {
    accounts: RwLock<HashMap<String, Arc<AccountCell>>>,
    on_first_lock: FirstLockHook,
}

impl Default for InMemoryLedgerStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryLedgerStore {
    pub fn new() -> Self {
        Self { accounts: RwLock::new(HashMap::new()), on_first_lock: Box::new(|_| {}) }
    }

    /// Calls `hook` with the id of the first account a transfer has locked, before it takes the second.
    #[cfg(test)]
    pub(super) fn with_first_lock_hook(hook: impl Fn(&str) + Send + Sync + 'static) -> Self {
        Self { accounts: RwLock::new(HashMap::new()), on_first_lock: Box::new(hook) }
    }

    fn cell(&self, id: &str) -> Option<Arc<AccountCell>> {
        self.accounts.read().unwrap_or_else(|e| e.into_inner()).get(id).cloned()
    }

    #[cfg(test)]
    pub(super) fn journal(&self, id: &str) -> Vec<JournalEntry> {
        let cell = self.cell(id).expect("unknown account");
        lock(&cell.state).journal.clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

impl LedgerStore for InMemoryLedgerStore {
    fn create_account(&self, id: &str, opening_balance: i64) -> bool {
        let mut state = CellState::default();
        state.post(OPENING_TRANSFER_ID, opening_balance);
        let cell = Arc::new(AccountCell { id: id.to_owned(), state: Mutex::new(state) });

        let mut accounts = self.accounts.write().unwrap_or_else(|e| e.into_inner());
        if accounts.contains_key(id) {
            return false;
        }
        accounts.insert(id.to_owned(), cell);
        true
    }

    fn find_account(&self, id: &str) -> Option<Account> {
        let cell = self.cell(id)?;
        let balance = lock(&cell.state).balance;
        Some(Account { id: cell.id.clone(), balance })
    }

    fn apply_transfer(&self, transfer_id: u64, request: &TransferRequest) -> Result<TransferOutcome, LedgerError> {
        let (Some(from), Some(to)) = (self.cell(request.from()), self.cell(request.to())) else {
            return Ok(TransferOutcome::rejected(transfer_id, request, RejectionReason::UnknownAccount));
        };

        let from_is_first = from.id < to.id;
        let (first, second) = if from_is_first { (&from, &to) } else { (&to, &from) };
        let first_guard = lock(&first.state);
        (self.on_first_lock)(&first.id);
        let second_guard = lock(&second.state);
        let (mut from_state, mut to_state) =
            if from_is_first { (first_guard, second_guard) } else { (second_guard, first_guard) };

        if from_state.balance < request.amount() {
            return Ok(TransferOutcome::rejected(transfer_id, request, RejectionReason::InsufficientFunds));
        }
        // Check before posting so an overflow cannot leave a half-applied transfer.
        if to_state.balance.checked_add(request.amount()).is_none() {
            return Err(LedgerError::Internal("balance overflow".to_owned()));
        }
        from_state.post(transfer_id, -request.amount());
        to_state.post(transfer_id, request.amount());
        Ok(TransferOutcome::completed(transfer_id, request))
    }
}
