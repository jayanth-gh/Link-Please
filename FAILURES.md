FAILURES — honest list of ways this system can still lose DMs, send duplicates, or report wrong numbers

1) No background sender (current trimmed build) — queued DMs never leave disk
- Condition: the current `app.py` stops after `/webhook` and does not run a background worker or sender.
- What happens: `deliveries` rows are created with `status='queued'` but nothing attempts `POST /v1/dm/send`. The grader will therefore see `queued` increase and `sent` remain zero.
- Consequence: DMs are effectively never sent (lost from the user's point of view) and `/stats` will under-report `sent` and over-report `queued`.
- Workaround / fix: start a single reliable background worker (or a scheduler) that reads `deliveries` and persists its progress to SQLite.

2) In-flight / process crash between send and reconcile
- Condition: a delivery POST is sent to the mock API and the process crashes before the app records the returned `dm_id` or before the next poll runs.
- What happens: we may have no local record that the remote API accepted the DM (missing `dm_id`), or we may have recorded `dm_id` but not yet polled to observe final `delivered` vs `failed`.
- Consequence: the DM might have been delivered or failed — we cannot be certain. If the worker later retries the same logical DM and the Idempotency-Key differs (or is not honored), the user may be duplicated; if the API honors the stable idempotency key there will be no duplicate but the local counts may be wrong.
- Mitigation: persist send attempts and `dm_id` atomically and run periodic reconciliation; still, a crash between network send and the atomic write is a hard window unless the HTTP client/DB write are made transactional (they are not).

3) Rate-limiter and multi-process deployment mismatch
- Condition: the implementation uses an in-process sliding-window limiter (10 requests / 60s). If you run more than one process (multiple WSGI workers, containers, or servers), each process enforces its own limiter.
- What happens: the global PseudoGram quota (10/60s per API key) can be exceeded across processes, producing `429` responses. These `429`s are retried and can cause bursts, longer queues, and confusing `queued`/`failed` counts.
- Consequence: increased retries, higher `queued` numbers, potential backoff storms, and metrics that do not match the grader's single-run truth (we may under- or over-count depending on how retries are scheduled).
- Practical note: assignment constraints forbid adding an external central limiter; the pragmatic approach is to run a single process worker or use a shared persistent counter (DB-based token bucket) if you need multi-process coordination.

4) Duplicate/metric inaccuracies due to idempotency assumptions and edge races
- Condition A: the code relies on `UNIQUE(rule_id, user_id)` to prevent sending the same rule twice. This works when all events hit the same database. In a multi-database or multi-process misconfigured deployment (or if code changes the uniqueness columns), duplicates can slip through.
- Condition B: the Idempotency-Key sent to the mock API is `"{rule_id}:{user_id}"`. If `user_id` or `rule_id` changes between attempts (bug, race, or a different insertion path), the API will treat retried requests as new and may deliver duplicates.
- Metric issue: `duplicates_blocked` is incremented when an `INSERT` hits the UNIQUE constraint. Under very high concurrency and complex error flows (e.g., a crash between the failed INSERT and the metric update), the counter can be off by ±1 or more.
- Consequence: the system can send duplicates in some rare edge cases, and `/stats` (duplicates_blocked, sent, failed) can disagree with the grader's ground truth.

Notes / how I verified these are realistic
- I built the implementation minimally to keep state in SQLite and to use `UNIQUE(rule_id, user_id)` and persistent `deliveries` rows. That reduces many race classes but does not eliminate crashes, multi-process races, or network-induced uncertainty.
- Each bullet above references concrete implementation choices (no worker, in-process limiter, unique constraint, idempotency-key format) and explains the exact window or condition that leads to the failure.

If you want, I can now:
- Re-enable the background worker and the delivery/polling logic and then re-run the simulator to observe concrete counts, or
- Replace the in-process limiter with a DB-backed token bucket (still single-process-safe) to reduce the multi-process risk.

