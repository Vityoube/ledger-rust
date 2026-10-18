# Concurrent ledger service

A double-entry ledger for a wallet product: accounts hold money, transfers move it between accounts,
and retried transfers are applied at most once. State is in memory only.

## Run

```
cargo run
```

The service listens on port 8080. Set `PORT` to change it. Run the tests with `cargo test`.

`http-tests/ledger.http` holds request-by-request API checks for IntelliJ's HTTP Client (select the
`local` environment, start the service, run the file top to bottom).

## API

Amounts are integers in minor units (for example cents). Responses are JSON.

| Request | Success | Other responses |
|---|---|---|
| `POST /accounts` `{"id":"alice","openingBalance":500}` (`openingBalance` optional, default 0) | `201` `{"id","balance"}` | `409` account exists, `400` bad input |
| `GET /accounts/{id}` | `200` `{"id","balance"}` | `404` unknown account |
| `POST /transfers` with header `Correlation-Id: <key>` and `{"from":"alice","to":"bob","amount":30}` | `201` completed | `422` rejected (`INSUFFICIENT_FUNDS` or `UNKNOWN_ACCOUNT`), `409` correlation id reused for a different request, `400` bad input or missing header |

Example:

```
curl -X POST localhost:8080/accounts -d '{"id":"alice","openingBalance":100}'
curl -X POST localhost:8080/accounts -d '{"id":"bob"}'
curl -X POST localhost:8080/transfers -H 'Correlation-Id: t-1' -d '{"from":"alice","to":"bob","amount":30}'
curl -X POST localhost:8080/transfers -H 'Correlation-Id: t-1' -d '{"from":"alice","to":"bob","amount":30}'   # same answer, applied once
curl localhost:8080/accounts/alice                                                                             # balance 70
```

## Guarantees

- **Atomic transfers.** A transfer debits one account and credits another, or it changes nothing.
  A rejected transfer (insufficient funds, unknown account) leaves all balances unchanged.
- **Balances never go negative.** The funds check and both postings happen while both accounts are locked.
- **No lost updates or double spends under concurrency.** Each account has its own lock; a transfer
  locks its two accounts in a fixed (id) order, which rules out deadlock. Tests cover 1,000 competing
  spends on one balance (exactly the funded number succeed) and 64,000 random transfers across 10
  accounts (money is conserved, no balance is negative, and every account's balance equals the sum
  of its journal).
- **Idempotent retries.** The first request with a correlation id claims it. A retry with the same id returns the
  original outcome, including a rejection, and never touches the balances. A retry that arrives while
  the original is still running waits for its result instead of applying the transfer a second time.
  Reusing a correlation id for a different body returns `409`.
- **Unrelated accounts proceed in parallel.** Transfers on disjoint account pairs never wait on each other.
  A test blocks one transfer inside its lock and checks that a transfer on other accounts completes.
- **Double-entry journal.** Each account keeps a list of postings. Every applied transfer posts a debit
  and an equal credit under the same transfer id.

## Layout

- `ledger::ledger`: the core, plain Rust with no web framework dependencies.
  - `Ledger`: the API. Validates input and handles idempotency.
  - `LedgerStore`: the storage port. Implementations must make `apply_transfer` atomic.
  - `InMemoryLedgerStore`: the in-memory implementation with per-account mutexes and a journal.
- `ledger::http`: a thin JSON adapter on axum. It contains no money logic. Ledger calls run on
  `spawn_blocking`, because a retry may block until the original transfer finishes.

To use a real database, implement `LedgerStore` and pass it to `Ledger::new`. Keep the same
atomicity contract and the same "no negative balance" check inside the database transaction.

## Trade-offs

- **Everything is in memory.** Restarting the process loses all accounts, balances and correlation ids.
  Durability is the job of the storage engine behind `LedgerStore`.
- **Idempotency records are never evicted.** Correlation ids stay in memory for the life of the process, so memory
  grows with the number of transfers. A production version would expire keys after a retention window
  and persist them in the same transaction as the transfer.
- **Idempotency state sits in `Ledger`, not in the store.** This keeps the store interface small, but a
  persistent store would also need the key claim persisted to get at-most-once across restarts.
- **Opening balances are not funded from a system account.** `createAccount` sets an opening balance directly,
  so money enters the system only there. Transfers conserve the total; the total changes only by account creation.
- **Per-account locks, not a global lock.** Throughput scales with the number of distinct accounts, but a single
  very hot account serialises all transfers that touch it.
- **Blocking HTTP handlers.** Each request runs on a thread from a cached pool and may wait on a lock. That is
  simple and correct, but it is not built for very high connection counts.
- **Rejections are stored as outcomes.** A retried rejection is replayed even if the account has since been funded.
  This matches "a repeated key returns the original outcome", at the cost of one stored record per key.
- **Amounts are `long` minor units.** There is no currency type, so callers must agree on the scale.

- **Balances are `i64`.** A transfer that would overflow the receiving balance fails with `500` before any posting, so it cannot half-apply, and its correlation id is released for retry.
