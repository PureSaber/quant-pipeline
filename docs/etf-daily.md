# Four ETF research and daily simulation

`python -m quant_pipeline.etf_research check|run --recipe frozen.yaml --output study`
uses the native equity research runner and retains its entire candidate family,
baseline, failures, registry and ledger files. `inspect --output study` verifies
the native registry and artifact identities. Historical results remain retrospective.

`python -m quant_pipeline.etf_daily status|refresh|observe|tick --profile daily.json`
uses operator-owned absolute paths. Profile keys (schema `quant.etf-daily/v1`):
`dataset_root`, `recipe`, `output_root`, `forward_config` (null before registration).
An optional `master_refresh` points to an operator-owned JSON configuration with
schema `quant.etf-master-refresh/v1`, absolute `source`, frozen declaration
`source_sha256`, and `guards` (official HTTPS URL, optional form POST data, and
expected response SHA-256). Configure current rule directories for both exchanges;
an empty page shell is not a rule directory. The refresh refetches every official
attachment and directory, blocks any changed bytes for review, then imports a new
native master bundle. Only current observation endpoints advance, to the actual
completed capture time. Original availability and all historical values remain
unchanged. It does not interpret new rules, certify source semantics or assert
daily trading/suspension status. Network failures during master verification block
the operation; partial evidence remains for inspection.
Seed the dataset with QDK `research_dataset build` for 510300, 510500, 159915, 588000.
The recipe must retain the original weekly strategy; only its input snapshot paths
are replaced. Output, source and account directories must be separate.

`refresh` acquires a new immutable QDK snapshot and runs the complete native
preflight. `observe` explicitly rechecks and executes/retries a previously registered
account through `forward_daily`; it does not register, assemble or replace an account.
`tick` uses the exchange calendar and Shanghai 16:30, 17:00 and 18:00 slots. Only
transport errors or a missing final bar are automatically retried. Earlier gaps,
historical revisions, identity changes and ledger errors stop the operation.
Interrupted account execution requires an explicit `observe` under the native
recovery contract. A stopped collection can be explicitly refreshed after inspection.

Every operation has a hashed receipt. A changed operator profile starts a separate
history view. One native study lock serializes operations; native account locking
and idempotency independently prevent duplicate observations. Completed observation
notifications occur once per date; repeated unchanged failures are quiet.

The daily view reads verified standard/v2 Parquet tables and the original risk and
execution diagnostics. Positions are simulated. Cost totals are grouped by native
currency and type; slippage remains a frozen assumption, never a real fill estimate.
Older artifacts without admitted-target diagnostics show targets as unavailable.
No return or account-success calculation is implemented in the Studio adapter.

The Studio repository supplies the UI. The workspace `profiles/etf-daily` profile
pins all installed repositories. Operate a single scheduler against `tick`; preserve
old paused accounts and obtain a recorded human candidate decision before activation.
Registration uses actual time and a future calendar session. Five natural sessions
are operational acceptance, not strategy validation.

## Bounded evening collection and timely orders

An optional `schedule` adds `timezone: Asia/Shanghai`, an ordered `slots` list
(this deployment: `16:30`, `17:00`, `18:00`, `21:00`, `22:00`), and
`collection_timeout_seconds: 600`. Profiles without it retain the legacy behavior.
Configured profiles allow entry only within the collection budget after a slot;
outside those windows all commands are read-only. Manual refresh and scheduled
tick share slot receipts. A successful refresh can feed observation without
another market fetch. Same-day completed observations are never repeated.

Collection runs in a killable isolated Python worker. Timeout stops the worker
and preserves partial evidence; only transport failures and missing final daily
bars are retryable. The last-slot failure becomes `collection_exhausted`.
Historical changes, corrupted artifacts and changed official evidence block.
Status exposes the next retry, market capture and preflight completion times.

Register new operational accounts with `research_paper promote
--require-timely-orders`. This freezes `prior-observation-before-open/v1` in the
account definition without changing strategy parameters or older accounts.
Before publishing a new observation, native v2 orders/fills are validated. Every
opening fill must match an immutable order in an earlier completed observation;
both its completion timestamp and native registry commit must precede that
session's 09:30 Shanghai opening. A late/missing witness or changed order rejects
the observation and retains the last successful ledger. Recovery cannot invent
orders for an opening that has already passed. Same-day reuse remains idempotent.
