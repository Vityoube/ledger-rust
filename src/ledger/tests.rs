use std::sync::Arc;

use super::store::JournalEntry;
use super::*;

struct Fixture {
    store: Arc<InMemoryLedgerStore>,
    ledger: Ledger,
}

/// Lets a test keep a handle on the store (for its journal) while the ledger owns it.
struct Shared(Arc<InMemoryLedgerStore>);

impl LedgerStore for Shared {
    fn create_account(&self, id: &str, opening_balance: i64) -> bool {
        self.0.create_account(id, opening_balance)
    }

    fn find_account(&self, id: &str) -> Option<Account> {
        self.0.find_account(id)
    }

    fn apply_transfer(&self, transfer_id: u64, request: &TransferRequest) -> Result<TransferOutcome, LedgerError> {
        self.0.apply_transfer(transfer_id, request)
    }
}

fn fixture() -> Fixture {
    let store = Arc::new(InMemoryLedgerStore::new());
    Fixture { ledger: Ledger::new(Box::new(Shared(Arc::clone(&store)))), store }
}

fn request(from: &str, to: &str, amount: i64) -> TransferRequest {
    TransferRequest::new(from, to, amount).unwrap()
}

fn balance(ledger: &Ledger, id: &str) -> i64 {
    ledger.account(id).unwrap().unwrap().balance
}

#[test]
fn creates_account_with_opening_balance_and_rejects_duplicates() {
    let Fixture { ledger, .. } = fixture();
    assert!(ledger.create_account("alice", 500).unwrap());
    assert!(!ledger.create_account("alice", 900).unwrap());
    assert_eq!(500, balance(&ledger, "alice"));
    assert!(ledger.account("nobody").unwrap().is_none());
}

#[test]
fn rejects_invalid_account_input() {
    let Fixture { ledger, .. } = fixture();
    assert!(matches!(ledger.create_account(" ", 0), Err(LedgerError::InvalidInput(_))));
    assert!(matches!(ledger.create_account("bob", -1), Err(LedgerError::InvalidInput(_))));
}

#[test]
fn completed_transfer_moves_money_and_keeps_total() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 500).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let outcome = ledger.transfer("k1", request("alice", "bob", 200)).unwrap();

    assert_eq!(Status::Completed, outcome.status);
    assert_eq!(None, outcome.reason);
    assert_eq!(300, balance(&ledger, "alice"));
    assert_eq!(200, balance(&ledger, "bob"));
}

#[test]
fn transfer_can_drain_account_to_exactly_zero() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    assert_eq!(Status::Completed, ledger.transfer("k1", request("alice", "bob", 100)).unwrap().status);
    assert_eq!(0, balance(&ledger, "alice"));
}

#[test]
fn insufficient_funds_rejects_and_changes_nothing() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 50).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let outcome = ledger.transfer("k1", request("alice", "bob", 51)).unwrap();

    assert_eq!(Status::Rejected, outcome.status);
    assert_eq!(Some(RejectionReason::InsufficientFunds), outcome.reason);
    assert_eq!(50, balance(&ledger, "alice"));
    assert_eq!(0, balance(&ledger, "bob"));
}

#[test]
fn unknown_account_rejects_and_changes_nothing() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 100).unwrap();

    let outcome = ledger.transfer("k1", request("alice", "ghost", 10)).unwrap();

    assert_eq!(Status::Rejected, outcome.status);
    assert_eq!(Some(RejectionReason::UnknownAccount), outcome.reason);
    assert_eq!(100, balance(&ledger, "alice"));
}

#[test]
fn rejects_malformed_transfer_requests() {
    let invalid = |r: Result<TransferRequest, LedgerError>| matches!(r, Err(LedgerError::InvalidInput(_)));
    assert!(invalid(TransferRequest::new("alice", "alice", 10)));
    assert!(invalid(TransferRequest::new("alice", "bob", 0)));
    assert!(invalid(TransferRequest::new("alice", "bob", -5)));
    assert!(invalid(TransferRequest::new(" ", "bob", 5)));

    let Fixture { ledger, .. } = fixture();
    assert!(matches!(ledger.transfer(" ", request("a", "b", 1)), Err(LedgerError::InvalidInput(_))));
}

#[test]
fn journal_records_both_legs_of_each_transfer() {
    let Fixture { ledger, store } = fixture();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let outcome = ledger.transfer("k1", request("alice", "bob", 30)).unwrap();

    let entry = |delta, balance_after| JournalEntry { transfer_id: outcome.transfer_id, delta, balance_after };
    assert_eq!(Some(entry(-30, 70)), store.journal("alice").last().cloned());
    assert_eq!(Some(entry(30, 30)), store.journal("bob").last().cloned());
}

#[test]
fn retry_with_same_key_returns_original_outcome_without_applying_again() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let first = ledger.transfer("k1", request("alice", "bob", 30)).unwrap();
    let retry = ledger.transfer("k1", request("alice", "bob", 30)).unwrap();

    assert_eq!(first, retry);
    assert_eq!(70, balance(&ledger, "alice"));
    assert_eq!(30, balance(&ledger, "bob"));
}

#[test]
fn retry_replays_rejection_even_if_funds_arrive_later() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 0).unwrap();
    ledger.create_account("bob", 50).unwrap();
    let rejected = ledger.transfer("k1", request("alice", "bob", 10)).unwrap();

    ledger.transfer("k2", request("bob", "alice", 50)).unwrap();
    let retry = ledger.transfer("k1", request("alice", "bob", 10)).unwrap();

    assert_eq!(Some(RejectionReason::InsufficientFunds), rejected.reason);
    assert_eq!(rejected, retry);
    assert_eq!(50, balance(&ledger, "alice"));
    assert_eq!(0, balance(&ledger, "bob"));
}

#[test]
fn reusing_key_for_different_request_is_rejected_and_applies_nothing() {
    let Fixture { ledger, .. } = fixture();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();
    ledger.create_account("carol", 0).unwrap();
    ledger.transfer("k1", request("alice", "bob", 10)).unwrap();

    let result = ledger.transfer("k1", request("alice", "carol", 10));

    assert!(matches!(result, Err(LedgerError::IdempotencyConflict(_))));
    assert_eq!(90, balance(&ledger, "alice"));
    assert_eq!(10, balance(&ledger, "bob"));
    assert_eq!(0, balance(&ledger, "carol"));
}

#[test]
fn balance_overflow_fails_without_half_applying_and_frees_the_key() {
    let Fixture { ledger, store } = fixture();
    ledger.create_account("alice", 10).unwrap();
    ledger.create_account("whale", i64::MAX).unwrap();

    let result = ledger.transfer("k1", request("alice", "whale", 1));

    assert!(matches!(result, Err(LedgerError::Internal(_))));
    assert_eq!(10, balance(&ledger, "alice"));
    assert_eq!(1, store.journal("alice").len());
    // The failed attempt released the key, so a retry is evaluated afresh rather than replayed.
    assert!(matches!(ledger.transfer("k1", request("alice", "whale", 1)), Err(LedgerError::Internal(_))));
}
