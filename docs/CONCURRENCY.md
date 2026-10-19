# Cross-device Concurrency, Duplicate Submissions, and Stale Version Handling

Quittance employs optimistic concurrency control (OCC) combined with keyed mutex locking to eliminate race conditions, double processing, and silent data loss across distributed clients, multi-tab sessions, wallet extensions, and automated retries.

---

## 1. Concurrency Architecture & Guarantees

### 1.1 Invariant Model

```
                    ┌────────────────────────┐
                    │ Invoice State: PENDING │ (version = N)
                    └───────────┬────────────┘
                                │
              ┌─────────────────┴─────────────────┐
              ▼                                   ▼
    [POST /invoices/:id/verify]          [POST /invoices/:id/cancel]
              │                                   │
   Lock (InvoiceLockManager)           Check (expectedVersion == N)
   Check payable (PENDING)                        │
   Horizon verification                           ▼
   Atomic status -> PAID               Atomic status -> CANCELLED
   version = N + 1                     version = N + 1
   Side-effects run ONCE                          │
              │                                   ▼
              ▼                          Release / Respond 200 OK
   Release / Respond 200 OK              (Racing sessions -> 409 CONFLICT)
   (Concurrent dupes -> 400 ALREADY_PAID)
```

1. **Monotonic Version Counter**: Every invoice carries an integer `version` starting at `1`, incremented on every state mutation (`CANCELLED`, `PAID`, or metadata update).
2. **Double-Processing Prevention (Issue #18)**: Concurrent duplicate verification submissions for the same invoice are serialized by `InvoiceLockManager`. Exactly one request executes the Horizon lookup and transitions state; all racing duplicate requests receive `400 INVOICE_ALREADY_PAID` without duplicate side-effects.
3. **No Lost Updates (Issue #284 / #274)**: Concurrent writes with stale version tokens are rejected with `409 CONFLICT`, returning the current authoritative version and attempted version.
4. **TxHash Uniqueness Guard**: A Stellar transaction hash can settle at most one invoice. Cross-invoice re-use is blocked by unique constraints (`UNIQUE (payment_tx_hash)`).

---

## 2. Handled Paths

| Path | Scenario | Expected Behavior | HTTP Code | Error Code |
|------|----------|-------------------|-----------|------------|
| **Normal** | Valid mutation with matching version | State transitions, `version` increments, side-effects execute once | `200 OK` | `null` |
| **Normal (Legacy)** | Older client omitting version token | Backwards compatibility: mutation succeeds without version assertion | `200 OK` | `null` |
| **Failure (Dupe)** | Concurrent duplicate verify requests | First request marks PAID; subsequent racing requests blocked by mutex | `400 Bad Request` | `INVOICE_ALREADY_PAID` |
| **Failure (Stale)** | Mutation with outdated `version` | Rejection of stale update; state unchanged | `409 Conflict` | `CONFLICT` |
| **Failure (Tx Collide)**| Re-use of already settled txHash | Rejection of duplicate settlement attempt | `400 Bad Request` | `DUPLICATE_TX_HASH` |
| **Failure (Invalid)** | Negative, float, or NaN version | Input validation fails before storage read | `400 Bad Request` | `INVALID_VERSION` |
| **Retry (409)** | Client catches 409 CONFLICT | Re-fetch latest invoice version via `GET /api/invoices/:id` and rebase | `200 OK` (on retry) | `null` |
| **Retry (Paid)** | Payer refresh after completed payment | Safe idempotent response returning proof retrieval instructions | `400 Bad Request` | `INVOICE_ALREADY_PAID` |
| **Authorization** | Unprivileged actor racing valid seller | Rejected immediately before lock acquisition or version mutation | `403 Forbidden` / `400` | `UNAUTHORIZED_SELLER` |

---

## 3. Storage Implementation Details

### 3.1 PostgreSQL Path (Atomic Conditional UPDATE)

In PostgreSQL, atomic state transitions are enforced in a single SQL statement:

```sql
UPDATE invoices
SET status = 'PAID',
    payment_tx_hash = $2,
    payer_public_key = $3,
    paid_at = NOW(),
    payer_name = $4,
    payer_email = $5,
    version = version + 1
WHERE id = $1
  AND status = 'PENDING'
  AND expires_at > NOW()
  AND ($6::integer IS NULL OR version = $6)
RETURNING *;
```

- **Row-Level Lock**: PostgreSQL acquires an exclusive `X` lock on the invoice row during `UPDATE`. Exactly one concurrent transaction matches `status = 'PENDING'` and `version = $6`.
- **Side-Effect Gating**: Side effects (notifications, audit log events, email delivery) trigger **only** when `RETURNING *` yields a modified row.
- **Rollback Guarantee**: If affected row count is `0`, the transaction rolls back without side effects. A secondary `SELECT version` determines if failure was caused by a stale version (raising `ConflictError`) or status expiration.

### 3.2 In-Memory (MVP) Path (Keyed Mutex Lock)

In the Node.js event loop, asynchronous operations (such as `await stellar.getTransaction(...)`) yield execution, creating race windows. To prevent interleaving:

- `InvoiceLockManager` maintains an in-memory keyed mutex per `invoiceId`.
- **Acquire Lock**: `const releaseLock = await invoiceLockManager.acquire(id);` before verification.
- **Release Lock**: Ensured via `finally { releaseLock(); }`, guaranteeing locks are released even on unhandled exceptions.
- **Post-Lock Check**: Once the lock is acquired, re-verifying `checkInvoiceIsPayable(invoice.status)` idempotently rejects requests that were queued while a previous request completed settlement.

---

## 4. Operational Edge Cases & Mitigation

### 4.1 Event Loop Yielding During Horizon RPC
- **Risk**: A slow Horizon response (~5 seconds during ledger close) allows multiple incoming verification requests to pile up in memory.
- **Mitigation**: `InvoiceLockManager` serializes requests per invoice ID. Requests 2..N wait on the lock. When Request 1 finishes and releases the lock, Request 2 immediately re-checks invoice status and exits with `INVOICE_ALREADY_PAID` in <1ms without hitting Horizon.

### 4.2 Process Crash or Worker Restart During In-Flight Lock
- **Risk**: Server crashes while an async handler holds a mutex.
- **Mitigation**:
  - In-memory locks reside in process memory; process restart cleanly wipes active locks without lingering deadlock.
  - In PostgreSQL, an uncommitted transaction releases all row locks immediately upon connection termination.

### 4.3 Client Multi-Tab Racing (Stale Version Storm)
- **Risk**: User has multiple tabs open modifying invoice metadata or cancelling while payment arrives.
- **Mitigation**: First write increments version from $V$ to $V+1$. Racing tab receives `409 CONFLICT` with JSON payload:
  ```json
  {
    "success": false,
    "error": "Invoice was modified by another session (current version: 2, attempted: 1). Please refresh and try again.",
    "code": "CONFLICT",
    "category": "LIFECYCLE",
    "retryable": true,
    "recoveryAction": "Refresh the invoice to get the latest version, then retry the operation."
  }
  ```

---

## 5. Recovery & Rollback Protocols

### 5.1 Client-Side Conflict Recovery (Rebase Protocol)
1. Catch HTTP `409 CONFLICT`.
2. Issue `GET /api/invoices/:id` (or inspect `ETag` header).
3. If `invoice.status !== 'PENDING'`, invoice entered a terminal state (`PAID`, `CANCELLED`, `EXPIRED`); inform user and halt retries.
4. If still `PENDING`, update local form state to `invoice.version` and prompt user to re-submit with latest version.

### 5.2 Server-Side Rollback Verification
- When an atomic UPDATE fails (0 rows returned):
  1. No `payment_events` row is written.
  2. No `audit_events` row is written.
  3. No emails or webhooks are dispatched.
  4. The existing invoice row remains unaltered.

---

## 6. Verification & Automated Test Commands

Run the full concurrency, replay, and duplicate submission test suites:

```bash
cd backend

# Run comprehensive concurrency tests for duplicate submissions & stale versions
node --import tsx --test tests/concurrency-duplicate-stale.test.ts

# Run unit OCC and optimistic lock tests
node --import tsx --test tests/concurrency.test.ts

# Run deterministic replay simulation tests
node --import tsx --test tests/deterministic-replay.test.ts
```
