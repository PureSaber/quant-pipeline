# Frozen ETF operations

`quant_pipeline.etf_operations` is an operations controller, not a new strategy engine.
Install it in a new reviewed environment. Its standalone worker runs with the original
account's interpreter using `-I -B`; it imports that environment's native collector,
preflight, forward guard, account writer and ledger verifier. Both installed trees and
all configured frozen inputs are checked. No runtime fallback, clock override,
account replacement or historical-fill override is provided.

## Operator interfaces

```text
python -I -B -m quant_pipeline.etf_operations status --config ABSOLUTE_CONFIG
python -I -B -m quant_pipeline.etf_operations tick --config ABSOLUTE_CONFIG
python -I -B -m quant_pipeline.etf_operations recover-plan --config ABSOLUTE_CONFIG --operation ID
python -I -B -m quant_pipeline.etf_operations recover-execute --config ABSOLUTE_CONFIG --plan PREVIEW_JSON
```

`status` is read-only. It returns the compatible `quant.etf-daily-status/v1` native
projection plus an `operations` object with schema `quant.etf-operations/v1`.
The new root stores `quant.etf-operation/v1` receipts separately from legacy daily
receipts. They reference native forward receipts, never create a parallel ledger.
Configuration requires explicit original profile and protected-file hashes, frozen
runtime and installation digest, separate evidence root, common maintenance root,
exact Windows task definition, execution switch and hashed continuation evidence.

Enable writes only after the original native rules accept continuation on an isolated
account copy with genuine current inputs. Data unavailable means continuation remains
unverified, not that the account is necessarily unrecoverable. Never move the original
observation start or add historical orders to obtain permission.

The configured exchange calendar and five ten-minute windows govern admission. A
network failure retries only at a later configured slot. Certificate failures, changed
official evidence, historical revisions, unknown errors and native timing failures
stop automatic progress. Collection has its original 600-second budget. The native
account process has a separate bounded budget; interrupted execution requires preview.

## Recovery and concurrency

The old daily `.research.lock`, native account lock and Workspace maintenance admission
protocol remain authoritative. Process creation identities identify unfinished child
workers; a stale PID or a remaining lock file is not proof that a writer is active.
Timeout handling terminates only the currently owned process tree. Recovery cannot
proceed while a native worker remains alive.

Recovery previews expire after 900 seconds and bind configuration, worker, account,
data and the latest receipt. Confirmation rechecks all bindings under the shared lock.
An already committed native observation is reconciled without repeated account writes.
When resolving a previously unfinished operations record, retain its exact original
JSON alongside the explicit interrupted resolution. Failed attempts remain inspectable.

## Acceptance and deployment

Only a verified running Windows task with an in-window original operation, completed
native observation and ledger verification can receive natural-session credit.
Manual execution and recovery receive none. A session receives natural credit
only when the native receipt proves a fresh ready-to-execute observation, including
its native attempt ID and result digest. A completed ledger that predates the scheduled
call can be reconciled but earns no natural-session credit.
Acceptance derives consecutive sessions
from the current configuration/worker version's receipts; gaps and previous versions
remain visible. Six consecutive trading days are an operations milestone, never proof
of strategy effectiveness. Legacy acceptance records are not rewritten.

Deploy tasks disabled, verify installed environments and continuation, drain the old
writer, switch its Codex automation to read-only monitoring, then enable Windows only
when all prerequisites pass. Before reverting the UI/runtime, disable and verify the
Windows writer. Preserve every newer account record during software rollback.
