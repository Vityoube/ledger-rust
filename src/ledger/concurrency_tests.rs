use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use super::store::{JournalEntry, OPENING_TRANSFER_ID};
use super::*;

const THREADS: usize = 32;

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

fn request(from: &str, to: &str, amount: i64) -> TransferRequest {
    TransferRequest::new(from, to, amount).unwrap()
}

fn balance(ledger: &Ledger, id: &str) -> i64 {
    ledger.account(id).unwrap().unwrap().balance
}

/// Runs each task on its own thread, released together, and returns the results in task order.
fn run_concurrently<T: Send>(tasks: Vec<Box<dyn FnOnce() -> T + Send + '_>>) -> Vec<T> {
    let gate = Barrier::new(tasks.len());
    thread::scope(|scope| {
        let gate = &gate;
        let handles: Vec<_> = tasks
            .into_iter()
            .map(|task| {
                scope.spawn(move || {
                    gate.wait();
                    task()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

/// A gate that blocks the first transfer to lock `blocked_id` until released.
struct Blocker {
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    store: InMemoryLedgerStore,
}

fn blocking_store(blocked_id: &'static str) -> Blocker {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let store = InMemoryLedgerStore::with_first_lock_hook(move |id| {
        if id == blocked_id {
            let _ = entered_tx.send(());
            // Returns when released, or when the test drops the sender.
            let _ = release_rx.lock().unwrap().recv();
        }
    });
    Blocker { entered, release, store }
}

#[test]
fn never_spends_more_than_available_under_contention() {
    let ledger = Ledger::default();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let ledger_ref = &ledger;
    let tasks: Vec<Box<dyn FnOnce() -> TransferOutcome + Send + '_>> = (0..1_000)
        .map(|n| {
            Box::new(move || ledger_ref.transfer(&format!("k{n}"), request("alice", "bob", 1)).unwrap())
                as Box<dyn FnOnce() -> TransferOutcome + Send + '_>
        })
        .collect();
    let outcomes = run_in_pool(THREADS, tasks);

    let completed = outcomes.iter().filter(|o| o.status == Status::Completed).count();
    assert_eq!(100, completed);
    assert_eq!(0, balance(&ledger, "alice"));
    assert_eq!(100, balance(&ledger, "bob"));
}

/// Runs many tasks on a fixed number of threads, all started together.
fn run_in_pool<T: Send>(threads: usize, tasks: Vec<Box<dyn FnOnce() -> T + Send + '_>>) -> Vec<T> {
    let queue = std::sync::Mutex::new(tasks.into_iter().enumerate().collect::<Vec<_>>());
    let gate = Barrier::new(threads);
    let results = std::sync::Mutex::new(Vec::new());
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                gate.wait();
                loop {
                    let next = queue.lock().unwrap().pop();
                    let Some((index, task)) = next else { break };
                    let value = task();
                    results.lock().unwrap().push((index, value));
                }
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|(index, _)| *index);
    results.into_iter().map(|(_, value)| value).collect()
}

#[test]
fn random_concurrent_transfers_conserve_money_and_never_go_negative() {
    let store = Arc::new(InMemoryLedgerStore::new());
    let ledger = Ledger::new(Box::new(Shared(Arc::clone(&store))));
    let accounts = 10usize;
    let opening = 1_000i64;
    for i in 0..accounts {
        ledger.create_account(&format!("acc{i}"), opening).unwrap();
    }

    let ledger_ref = &ledger;
    let workers: Vec<Box<dyn FnOnce() -> usize + Send + '_>> = (0..THREADS)
        .map(|seed| {
            Box::new(move || {
                let mut rng = XorShift(seed as u64 + 1);
                let mut completed = 0usize;
                for j in 0..2_000 {
                    let from = rng.below(accounts as u64) as usize;
                    let to = (from + 1 + rng.below(accounts as u64 - 1) as usize) % accounts;
                    let amount = 1 + rng.below(300) as i64;
                    let outcome = ledger_ref
                        .transfer(&format!("t{seed}-{j}"), request(&format!("acc{from}"), &format!("acc{to}"), amount))
                        .unwrap();
                    if outcome.status == Status::Completed {
                        completed += 1;
                    }
                }
                completed
            }) as Box<dyn FnOnce() -> usize + Send + '_>
        })
        .collect();
    let completed_total: usize = run_concurrently(workers).into_iter().sum();

    let mut total = 0;
    let mut delta_by_transfer: HashMap<u64, i64> = HashMap::new();
    let mut postings = 0;
    for i in 0..accounts {
        let id = format!("acc{i}");
        let balance = balance(&ledger, &id);
        assert!(balance >= 0, "{id} went negative: {balance}");
        total += balance;

        let journal: Vec<JournalEntry> = store.journal(&id);
        let journal_sum: i64 = journal.iter().map(|e| e.delta).sum();
        assert_eq!(balance, journal_sum, "lost update on {id}");
        for e in journal.iter().filter(|e| e.transfer_id != OPENING_TRANSFER_ID) {
            *delta_by_transfer.entry(e.transfer_id).or_default() += e.delta;
            postings += 1;
        }
    }

    assert_eq!(accounts as i64 * opening, total, "money was created or destroyed");
    assert!(delta_by_transfer.values().all(|sum| *sum == 0));
    assert_eq!(completed_total * 2, postings);
}

#[test]
fn concurrent_retries_with_same_key_apply_once_and_share_outcome() {
    let ledger = Ledger::default();
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let ledger_ref = &ledger;
    let retries: Vec<Box<dyn FnOnce() -> TransferOutcome + Send + '_>> = (0..THREADS)
        .map(|_| {
            Box::new(move || ledger_ref.transfer("same-key", request("alice", "bob", 40)).unwrap())
                as Box<dyn FnOnce() -> TransferOutcome + Send + '_>
        })
        .collect();
    let outcomes = run_concurrently(retries);

    let mut ids: Vec<u64> = outcomes.iter().map(|o| o.transfer_id).collect();
    ids.dedup();
    assert_eq!(1, ids.len());
    assert!(outcomes.iter().all(|o| o.status == Status::Completed));
    assert_eq!(60, balance(&ledger, "alice"));
    assert_eq!(40, balance(&ledger, "bob"));
}

#[test]
fn retry_arriving_while_original_is_in_flight_waits_for_its_outcome() {
    let Blocker { entered, release, store } = blocking_store("alice");
    let ledger = Ledger::new(Box::new(store));
    ledger.create_account("alice", 100).unwrap();
    ledger.create_account("bob", 0).unwrap();

    let ledger = &ledger;
    thread::scope(|scope| {
        let original = scope.spawn(move || ledger.transfer("k", request("alice", "bob", 40)).unwrap());
        entered.recv_timeout(Duration::from_secs(5)).expect("original never reached its lock");

        let (done_tx, done_rx) = mpsc::channel();
        let retry = scope.spawn(move || {
            let outcome = ledger.transfer("k", request("alice", "bob", 40)).unwrap();
            done_tx.send(()).unwrap();
            outcome
        });
        assert!(done_rx.recv_timeout(Duration::from_millis(200)).is_err(), "retry returned before the original");

        release.send(()).unwrap();
        assert_eq!(original.join().unwrap(), retry.join().unwrap());
    });
    assert_eq!(60, balance(&ledger, "alice"));
    assert_eq!(40, balance(&ledger, "bob"));
}

#[test]
fn transfers_on_unrelated_accounts_do_not_wait_for_a_blocked_transfer() {
    let Blocker { entered, release, store } = blocking_store("alice");
    let ledger = Ledger::new(Box::new(store));
    for id in ["alice", "bob", "carol", "dave"] {
        ledger.create_account(id, 100).unwrap();
    }

    let ledger = &ledger;
    thread::scope(|scope| {
        let blocked = scope.spawn(move || ledger.transfer("k1", request("alice", "bob", 10)).unwrap());
        entered.recv_timeout(Duration::from_secs(5)).expect("blocked transfer never reached its lock");

        let (tx, rx) = mpsc::channel();
        scope.spawn(move || tx.send(ledger.transfer("k2", request("carol", "dave", 10)).unwrap()).unwrap());
        let other = rx.recv_timeout(Duration::from_secs(5)).expect("unrelated transfer waited");
        assert_eq!(Status::Completed, other.status);

        release.send(()).unwrap();
        assert_eq!(Status::Completed, blocked.join().unwrap().status);
    });
}

/// A small deterministic generator, so the test needs no `rand` dependency.
struct XorShift(u64);

impl XorShift {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}
