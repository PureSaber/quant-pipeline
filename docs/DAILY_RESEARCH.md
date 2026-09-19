# Close-of-day research workflow

Use the unified Python 3.10 profile in `quant-workspace/profiles/daily-research`.
Installing quant-pipeline alone does not install the optional strategy, account or report apps.

```sh
python -m quant_pipeline.daily --config configs/daily_research.yaml
```

The YAML root is relative to the config file. Every invocation records subprocess
exit codes, stdout/stderr and their hashes under `output/operations/<invocation>`.
The steps are decision generation, experiment indexing, optional account import,
then dashboard regeneration. Configure `account_config` only after supplying real
statements; the repository example is synthetic. `operation-latest.json` is the
operational result; `latest.json` remains the decision consumer contract.

Failure refreshes the latest decision to blocked, including a producer crash that
published nothing. The report still runs and shows that failure. A report failure
marks the operation failed; existing HTML is only a historical snapshot, so check
`operation-latest.json` before using it. A simultaneous invocation is rejected by
an exclusive lock. On a hard process/host crash, verify no producer is running
before removing `.pipeline.lock` or the producer's `.decision.lock`; do not run
two writers against one account. Stale running trials remain visible for audit.

Repeat with `--inputs <immutable-input-directory> --as-of YYYY-MM-DD` for offline
verification. Same-session paper signals are frozen. After a missed day, a fresh
complete input window replays the existing account through the newly available
exchange sessions. Missing bars, revised history, changed config/code, and action
revisions block continuation instead of resetting the account silently.

This command does not install an operating-system schedule. Run after the official
session data are available; do not interpret an intraday/stale card as fresh.
The default ST/halt mode is advisory and labels missing evidence unverified.
Use `trading_status: required` for a profile that must refuse missing current
observations. Even that feed does not certify full historical tradability.
