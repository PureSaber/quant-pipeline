# Project-bound simulation adapter

`python -m quant_pipeline.project_registration {preview,commit,inspect,list}
--config <server-owned-json>` accepts a bounded JSON request on stdin (or
`--request-json`). The configuration uses `quant.project-registration/v1`, with
absolute disjoint `runs_root`, `accounts_root`, `state_root`, and a
`preview_ttl_seconds` integer from 60 to 3600.

Preview request fields: `project_id`, `revision`, `run_id`, `candidate`, `start`,
`end`. Commit/inspect request fields: `project_id`, `preview_id`. List accepts only
`project_id`. No form-supplied path or command is accepted.

The source must be a successful, explicitly project-bound native ETF run with a
complete collected artifact manifest, unchanged project identity and a connected
recipe revision. Native `load_study` verifies registry and candidate artifacts.
`research_paper.prepare_promotion` shares the existing promotion rules without
writing an account; native preflight must also pass. The preview binds source,
input, code, configuration and future observation window. Confirmation rechecks
all prerequisites and rejects stale previews.

The adapter uses native promotion and account inspection, with timely-order policy
enabled. Atomic publication from `.pending-*` and an interprocess lock protect
duplicate submission. A committed DB can recover failed report publication without
registering again; incomplete failed directories are preserved for diagnosis.
Receipts preserve attempt failures. Existing native promotion behavior remains
compatible, and no account observation or scheduler is invoked.

Install this module in the new registration/research runtime. Keep any older frozen
daily interpreter at its original version. The Studio enrichment reads its v1 status
and verified historical receipts without requiring this adapter in that runtime.
