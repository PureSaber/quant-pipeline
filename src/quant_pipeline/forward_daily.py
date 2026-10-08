"""Manual, fail-closed daily orchestration for registered forward-paper accounts."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from quant_lab.research import canonical, digest, file_hash, load_recipe
from quant_lab.trials import TrialRegistry

from quant_pipeline import research_paper as paper

SCHEMA_VERSION = "quant.forward-daily/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}")
_CONFIG_KEYS = {
    "schema_version",
    "approval",
    "account",
    "account_id",
    "definition_sha256",
    "input_recipe",
    "input_recipe_sha256",
    "receipt_dir",
    "market",
}
_MARKET_KEYS = {"timezone", "session_close", "calendar_source"}
_NATIVE_MARKET = {
    "timezone": "Asia/Shanghai",
    "session_close": "15:00",
    "calendar_source": "input_bundle",
}


class ForwardDailyError(ValueError):
    """A stable, user-facing workflow error with a machine-readable code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ForwardDailyConfig:
    path: Path
    sha256: str
    approval: str
    account: Path
    account_id: str
    definition_sha256: str
    input_recipe: Path
    input_recipe_sha256: str
    receipt_dir: Path
    receipt_account_root: Path
    timezone: str
    session_close: time


def _absolute(base: Path, value: str) -> Path:
    path = Path(value)
    return (base / path).resolve() if not path.is_absolute() else path.resolve()


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ForwardDailyError("CONFIG_INVALID", f"{key} must be a non-empty string")
    return value


def _require_separate_path(path: Path, account: Path, name: str) -> None:
    path = path.resolve()
    account = account.resolve()
    if path == account or account in path.parents or path in account.parents:
        raise ForwardDailyError(
            "CONFIG_INVALID", f"{name} must be separate from the frozen account tree"
        )


def load_config(path: Path) -> ForwardDailyConfig:
    """Load the strict operator allowlist without touching the paper account."""
    path = path.resolve()
    try:
        raw = path.read_bytes()
        payload = yaml.safe_load(raw)
    except (OSError, yaml.YAMLError) as exc:
        raise ForwardDailyError("CONFIG_UNREADABLE", f"Cannot read config: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != _CONFIG_KEYS:
        raise ForwardDailyError("CONFIG_INVALID", "Forward config keys do not match v1 schema")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ForwardDailyError("CONFIG_INVALID", "Unsupported forward daily schema")
    approval = _required_string(payload, "approval")
    if approval not in {"paused", "approved"}:
        raise ForwardDailyError("CONFIG_INVALID", "approval must be paused or approved")
    account_id = _required_string(payload, "account_id")
    definition_sha256 = _required_string(payload, "definition_sha256")
    recipe_sha256 = _required_string(payload, "input_recipe_sha256")
    if not _SHA256.fullmatch(definition_sha256) or not _SHA256.fullmatch(recipe_sha256):
        raise ForwardDailyError("CONFIG_INVALID", "Configured hashes must be lowercase SHA-256")
    market = payload.get("market")
    if not isinstance(market, dict) or set(market) != _MARKET_KEYS:
        raise ForwardDailyError("CONFIG_INVALID", "market keys do not match v1 schema")
    if market != _NATIVE_MARKET:
        raise ForwardDailyError(
            "MARKET_CONTRACT_MISMATCH",
            "Market contract must match the native paper-account close boundary",
        )
    close = time.fromisoformat(market["session_close"])
    account = _absolute(path.parent, _required_string(payload, "account"))
    receipts = _absolute(path.parent, _required_string(payload, "receipt_dir"))
    receipt_account_root = (receipts / account_id).resolve()
    _require_separate_path(receipt_account_root, account, "per-account receipt root")
    return ForwardDailyConfig(
        path=path,
        sha256=file_hash(path),
        approval=approval,
        account=account,
        account_id=account_id,
        definition_sha256=definition_sha256,
        input_recipe=_absolute(path.parent, _required_string(payload, "input_recipe")),
        input_recipe_sha256=recipe_sha256,
        receipt_dir=receipts,
        receipt_account_root=receipt_account_root,
        timezone=market["timezone"],
        session_close=close,
    )


def _check(checks: list[dict[str, Any]], name: str, status: str, **evidence: Any) -> None:
    checks.append({"check": name, "status": status, "evidence": evidence})


def _result(
    *,
    schema: str,
    now: datetime,
    as_of: str,
    state: str,
    reason: str,
    checks: list[dict[str, Any]],
    identity: dict[str, Any],
    action: str,
    retry_allowed: bool = False,
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "read_only": True,
        "checked_at": now.astimezone(timezone.utc).isoformat(),
        "as_of": as_of,
        "state": state,
        "reason": reason,
        "action": action,
        "retry_allowed": retry_allowed,
        "checks": checks,
        "identity": identity,
        "limitations": [
            "approval is an operator switch, not independent evidence that the research is true",
            "the exchange calendar and captured_at evidence are required; weekdays are never inferred",
            "this workflow has no scheduler, broker connection, or automatic retry",
        ],
    }


def _lock_held(root: Path) -> bool:
    """Probe an existing native lock without creating or changing account files."""
    lock = root / ".research.lock"
    if not lock.is_file():
        return False
    try:
        with lock.open("r+b") as handle:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(handle, fcntl.LOCK_UN)
        return False
    except OSError:
        return True


def _attempt_state(events: list[dict[str, Any]], as_of: str) -> tuple[str | None, dict | None]:
    latest: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for event in events:
        attempt = event["attempt_id"]
        if attempt not in latest:
            order.append(attempt)
        latest[attempt] = event
    for attempt in reversed(order):
        event = latest[attempt]
        payload = event["payload"]
        target = payload.get("as_of", payload.get("context", {}).get("as_of"))
        if target == as_of:
            return event["status"], event
    return None, None


def _session_close(day: pd.Timestamp, config: ForwardDailyConfig) -> datetime:
    local = datetime.combine(day.date(), config.session_close, ZoneInfo(config.timezone))
    return local


def _covered_observation(observations: list[tuple[dict, Path]], as_of: str) -> dict | None:
    for result, _ in observations:
        if result["as_of"] >= as_of:
            return result
    return None


def _non_input_recipe(recipe: dict[str, Any]) -> dict[str, Any]:
    value = deepcopy(recipe)
    value.pop("inputs", None)
    return value


def assess(
    config_path: Path,
    as_of: str,
    *,
    now: datetime | None = None,
    schema: str = "quant.forward-daily-plan/v1",
    _lock_owned: bool = False,
    _binding_only: bool = False,
) -> dict[str, Any]:
    """Build a read-only execution decision from frozen and point-in-time evidence."""
    try:
        date.fromisoformat(as_of)
    except ValueError as exc:
        raise ForwardDailyError("AS_OF_INVALID", "as_of must be an ISO calendar date") from exc
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ForwardDailyError("CLOCK_INVALID", "A timezone-aware clock is required")
    config = load_config(config_path)
    checks: list[dict[str, Any]] = []
    identity: dict[str, Any] = {
        "config_sha256": config.sha256,
        "workflow_source_sha256": file_hash(Path(__file__)),
        "account_id": config.account_id,
        "definition_sha256": config.definition_sha256,
        "input_recipe_sha256": config.input_recipe_sha256,
    }

    try:
        account = paper.load_account(config.account)
        if account["account_id"] != config.account_id:
            raise ForwardDailyError("ACCOUNT_IDENTITY_MISMATCH", "Configured account_id differs")
        if account["definition_sha256"] != config.definition_sha256:
            raise ForwardDailyError(
                "ACCOUNT_IDENTITY_MISMATCH", "Configured definition hash differs"
            )
        spec = account["definition"]
        inspection = paper.inspect_account(config.account, now=now)
        _check(checks, "registered_account", "passed", created_at=account["created_at"])
        identity.update(
            source_study_sha256=spec["source_study_sha256"],
            source_definition=spec["source_definition"],
            candidate=spec["parameters"][0]["name"],
            frozen_code_identity=spec["code_identity"],
        )
    except ForwardDailyError:
        raise
    except (FileNotFoundError, OSError) as exc:
        _check(checks, "registered_account", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="data_missing",
            reason=f"Registered account evidence is unavailable: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )
    except Exception as exc:  # noqa: BLE001 - external verifier errors fail closed
        _check(checks, "registered_account", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="verification_failed",
            reason=f"Registered account verification failed: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )

    try:
        runtime_identity = paper.equity_code_identity()
        identity["runtime_code_identity"] = runtime_identity
        if runtime_identity != spec["code_identity"]:
            raise ValueError("runtime code identity differs from the frozen account")
        _check(checks, "native_code_identity", "passed")
    except Exception as exc:  # noqa: BLE001 - code identity spans installed packages
        _check(checks, "native_code_identity", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="verification_failed",
            reason=f"Native code identity verification failed: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )

    try:
        if file_hash(config.input_recipe) != config.input_recipe_sha256:
            raise ValueError("input recipe bytes differ from the approved hash")
        recipe = load_recipe(config.input_recipe)
        if _non_input_recipe(recipe) != _non_input_recipe(spec["recipe"]):
            raise ValueError("input recipe changes frozen research settings")
        inputs = recipe["inputs"]
        if set(inputs) != set(spec["recipe"]["inputs"]):
            raise ValueError("input domains differ from the frozen recipe")
        _check(checks, "approved_input_recipe", "passed", path=str(config.input_recipe))
    except FileNotFoundError as exc:
        _check(checks, "approved_input_recipe", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="data_missing",
            reason=f"Approved input recipe is unavailable: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )
    except Exception as exc:  # noqa: BLE001 - recipe validation errors fail closed
        _check(checks, "approved_input_recipe", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="verification_failed",
            reason=f"Approved input recipe verification failed: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )

    observations = paper.observations(config.account, account)
    covered = _covered_observation(observations, as_of)
    if covered is not None and not _binding_only:
        _check(checks, "native_observation", "passed", covered_by=covered["as_of"])
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="completed",
            reason="The native verified observation already covers this session",
            checks=checks,
            identity=identity,
            action="none",
        )
    if inspection["state"] == "sealed":
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="sealed",
            reason="The forward account is sealed",
            checks=checks,
            identity=identity,
            action="none",
        )

    events = TrialRegistry(config.account / "account.db", read_only=True).history(config.account_id)
    attempt_status, attempt = _attempt_state(events, as_of)
    if attempt_status == "running" and not _lock_owned and _lock_held(config.account):
        _check(checks, "exclusive_account_writer", "blocked", attempt_id=attempt["attempt_id"])
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="running",
            reason="The native account writer still holds the account lock",
            checks=checks,
            identity=identity,
            action="none",
        )

    if config.approval != "approved":
        _check(checks, "operator_switch", "blocked", approval=config.approval)
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="paused",
            reason="The manual forward workflow is paused",
            checks=checks,
            identity=identity,
            action="none",
        )
    _check(checks, "operator_switch", "passed", approval=config.approval)

    if not spec["holdout_start"] <= as_of <= spec["holdout_end"]:
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="outside_window",
            reason="Requested session is outside the registered forward interval",
            checks=checks,
            identity=identity,
            action="none",
        )

    try:
        if paper.input_lineage(inputs) != spec["input_lineage"]:
            raise ValueError("input lineage differs from the promoted dataset")
        if paper.input_prefix(inputs, spec["initial_input_cutoff"]) != spec["initial_input_prefix"]:
            raise ValueError("historical inputs changed after promotion")
        if observations:
            previous = observations[-1][0]
            if paper.input_prefix(inputs, previous["as_of"]) != previous["input_prefix"]:
                raise ValueError("previously observed input history changed")
        input_identity = paper.input_identity({"inputs": inputs})
        identity["input_identity"] = input_identity
        _check(checks, "input_identity_and_prefix", "passed")
    except FileNotFoundError as exc:
        _check(checks, "input_identity_and_prefix", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="data_missing",
            reason=f"Required input evidence is missing: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )
    except Exception as exc:  # noqa: BLE001 - input providers expose multiple error types
        _check(checks, "input_identity_and_prefix", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="verification_failed",
            reason=f"Input identity verification failed: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )

    try:
        from a_share_multifactor.decision_workflow import load_inputs

        manifest, frames = load_inputs(Path(inputs["bundle"]))
        captured_raw = manifest.get("captured_at")
        if not isinstance(captured_raw, str):
            raise ForwardDailyError(
                "CAPTURE_TIME_MISSING", "Input manifest lacks timezone-aware captured_at evidence"
            )
        captured = pd.Timestamp(captured_raw)
        if captured.tzinfo is None:
            raise ForwardDailyError(
                "CAPTURE_TIME_MISSING", "Input manifest captured_at must be timezone-aware"
            )
        captured_at = captured.to_pydatetime()
        if captured_at > now:
            raise ForwardDailyError("CAPTURE_TIME_INVALID", "Input capture time is in the future")
        calendar = pd.DatetimeIndex(pd.to_datetime(frames["calendar"].date)).normalize().unique()
        calendar = pd.DatetimeIndex(calendar).sort_values()
        if len(calendar) == 0:
            raise ForwardDailyError("CALENDAR_MISSING", "Exchange calendar is empty")
        local_now = now.astimezone(ZoneInfo(config.timezone))
        local_capture = captured_at.astimezone(ZoneInfo(config.timezone))
        coverage_day = max(local_now.date(), local_capture.date())
        if calendar.max().date() < coverage_day:
            raise ForwardDailyError(
                "CALENDAR_NOT_COVERED",
                "Exchange calendar does not cover the current/capture date",
            )
        target = pd.Timestamp(as_of)
        if target not in calendar:
            _check(checks, "exchange_session", "blocked", calendar_max=str(calendar.max().date()))
            return _result(
                schema=schema,
                now=now,
                as_of=as_of,
                state="market_closed",
                reason="The covered exchange calendar does not list this date as a session",
                checks=checks,
                identity=identity,
                action="none",
            )
        target_close = _session_close(target, config)
        if now < target_close:
            _check(checks, "session_close", "blocked", close=target_close.isoformat())
            return _result(
                schema=schema,
                now=now,
                as_of=as_of,
                state="not_due",
                reason="The explicit market close has not passed",
                checks=checks,
                identity=identity,
                action="none",
            )
        if captured_at < target_close:
            raise ForwardDailyError(
                "SESSION_DATA_NOT_CAPTURED",
                "Input snapshot was captured before the requested session closed",
            )
        closed_now = [day for day in calendar if _session_close(day, config) <= now]
        closed_at_capture = [day for day in calendar if _session_close(day, config) <= captured_at]
        if not closed_now or not closed_at_capture:
            raise ForwardDailyError("CALENDAR_MISSING", "No completed session can be established")
        if target != closed_now[-1] or target != closed_at_capture[-1]:
            return _result(
                schema=schema,
                now=now,
                as_of=as_of,
                state="historical_backfill_blocked",
                reason=(
                    "Requested session is not the latest completed session at both capture and run "
                    "time; it cannot be introduced as new forward evidence"
                ),
                checks=checks,
                identity=identity,
                action="none",
            )
        for name in ("raw", "adjusted", "benchmark"):
            frame = frames[name]
            if frame.empty or not pd.to_datetime(frame.date).dt.normalize().eq(target).any():
                raise ForwardDailyError(
                    "SESSION_DATA_MISSING", f"{name} has no rows for the requested session"
                )
        identity["input_captured_at"] = captured_at.isoformat()
        _check(
            checks,
            "calendar_capture_and_session_data",
            "passed",
            captured_at=captured_at.isoformat(),
            session_close=target_close.isoformat(),
        )
    except ForwardDailyError as exc:
        _check(checks, "calendar_capture_and_session_data", "failed", code=exc.code)
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="data_missing",
            reason=str(exc),
            checks=checks,
            identity=identity,
            action="none",
        )
    except (FileNotFoundError, KeyError, OSError) as exc:
        _check(checks, "calendar_capture_and_session_data", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="data_missing",
            reason=f"Session data evidence is unavailable: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )
    except Exception as exc:  # noqa: BLE001 - data verification must fail closed
        _check(checks, "calendar_capture_and_session_data", "failed", error=str(exc))
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="verification_failed",
            reason=f"Session data verification failed: {exc}",
            checks=checks,
            identity=identity,
            action="none",
        )

    if attempt_status == "running":
        _check(checks, "exclusive_account_writer", "recoverable", attempt_id=attempt["attempt_id"])
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="interrupted",
            reason="A native attempt has no terminal event and the account lock is free",
            checks=checks,
            identity=identity,
            action="run",
            retry_allowed=True,
        )
    if attempt_status == "failed":
        return _result(
            schema=schema,
            now=now,
            as_of=as_of,
            state="retryable_failure",
            reason="The prior native failure is preserved; an explicit retry is allowed",
            checks=checks,
            identity=identity,
            action="run",
            retry_allowed=True,
        )
    return _result(
        schema=schema,
        now=now,
        as_of=as_of,
        state="ready",
        reason="All registered identity, calendar, capture, and session-data checks passed",
        checks=checks,
        identity=identity,
        action="run",
    )


def _write_receipt(config: ForwardDailyConfig, value: dict[str, Any]) -> tuple[Path, str]:
    root = config.receipt_account_root.resolve()
    _require_separate_path(root, config.account, "per-account receipt root")
    directory = (root / value["as_of"]).resolve()
    if root != directory and root not in directory.parents:
        raise ForwardDailyError("CONFIG_INVALID", "receipt path escapes its per-account root")
    _require_separate_path(directory, config.account, "final receipt directory")
    directory.mkdir(parents=True, exist_ok=True)
    directory = directory.resolve()
    if root != directory and root not in directory.parents:
        raise ForwardDailyError("CONFIG_INVALID", "receipt path escapes its per-account root")
    _require_separate_path(directory, config.account, "final receipt directory")
    identifier = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex
    path = directory / f"{identifier}.json"
    payload = deepcopy(value)
    receipt_sha256 = digest(payload)
    payload["receipt_sha256"] = receipt_sha256
    encoded = canonical(payload).encode("utf-8")
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return path, receipt_sha256


def run(
    config_path: Path,
    as_of: str,
    *,
    now: datetime | None = None,
    executor=None,
) -> dict[str, Any]:
    """Execute at most one native observation attempt and write an external audit receipt."""
    now = now or datetime.now(timezone.utc)
    config = load_config(config_path)
    plan = assess(config_path, as_of, now=now)
    base = {
        "schema_version": "quant.forward-daily-receipt/v1",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "as_of": as_of,
        "plan_state": plan["state"],
        "identity": plan["identity"],
        "checks": plan["checks"],
    }
    if plan["identity"].get("config_sha256") != config.sha256:
        receipt = {
            **base,
            "outcome": "failed",
            "state": "execution_binding_changed",
            "reason": "Forward config changed while the execution plan was being assessed",
            "error_type": "ForwardDailyError",
        }
        path, receipt_sha256 = _write_receipt(config, receipt)
        return {**receipt, "receipt": str(path), "receipt_sha256": receipt_sha256}
    if plan["action"] != "run":
        outcome = "success" if plan["state"] == "completed" else "blocked"
        receipt = {**base, "outcome": outcome, "state": plan["state"], "reason": plan["reason"]}
        path, receipt_sha256 = _write_receipt(config, receipt)
        return {**receipt, "receipt": str(path), "receipt_sha256": receipt_sha256}
    expected_identity = deepcopy(plan["identity"])
    base["execution_binding_sha256"] = digest(expected_identity)

    def execution_gate(stage: str, native_input_identity: dict[str, Any]) -> None:
        current = assess(
            config_path,
            as_of,
            now=now,
            _lock_owned=True,
            _binding_only=True,
        )
        if current["action"] != "run":
            raise ForwardDailyError(
                "EXECUTION_BINDING_CHANGED",
                f"Execution gate {stage} is blocked by current state {current['state']}",
            )
        if expected_identity.get("input_identity") != native_input_identity:
            raise ForwardDailyError(
                "EXECUTION_BINDING_CHANGED",
                f"Native input identity differs from the execution plan at {stage}",
            )
        if current["identity"] != expected_identity:
            raise ForwardDailyError(
                "EXECUTION_BINDING_CHANGED",
                f"Execution identity changed at {stage}",
            )

    try:
        recipe = load_recipe(config.input_recipe)
        observed = paper.observe(
            config.account,
            recipe["inputs"],
            as_of=as_of,
            now=now,
            executor=executor,
            execution_gate=execution_gate,
        )
        if observed.get("input_identity") != expected_identity.get("input_identity"):
            raise ForwardDailyError(
                "EXECUTION_BINDING_CHANGED",
                "Native result input identity differs from the execution plan",
            )
        inspection = paper.inspect_account(config.account, now=now)
        if inspection["latest"] is None or inspection["latest"]["as_of"] != observed["as_of"]:
            raise ValueError("Native ledger verification did not find the completed observation")
        events = TrialRegistry(config.account / "account.db", read_only=True).history(
            config.account_id
        )
        terminal = next(
            event
            for event in reversed(events)
            if event["status"] == "completed"
            and json.loads(
                (config.account / event["payload"]["result"]).read_text(encoding="utf-8")
            )["as_of"]
            == as_of
        )
        receipt = {
            **base,
            "outcome": "success",
            "state": "completed",
            "reason": "Native observation and ledger artifacts verified",
            "native_attempt_id": terminal["attempt_id"],
            "native_result_sha256": terminal["payload"]["sha256"],
        }
    except Exception as exc:  # noqa: BLE001 - every execution failure needs a receipt
        receipt = {
            **base,
            "outcome": "failed",
            "state": "execution_failed",
            "reason": str(exc),
            "error_type": type(exc).__name__,
        }
    path, receipt_sha256 = _write_receipt(config, receipt)
    return {**receipt, "receipt": str(path), "receipt_sha256": receipt_sha256}


def _exit_code(command: str, value: dict[str, Any]) -> int:
    if command in {"status", "plan"}:
        return 0
    if value.get("outcome") == "success":
        return 0
    if value.get("outcome") == "failed":
        return 5
    return 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "plan", "run"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--as-of", required=True, help="Explicit exchange calendar date")
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            value = run(args.config, args.as_of)
        else:
            value = assess(
                args.config,
                args.as_of,
                schema=f"quant.forward-daily-{args.command}/v1",
            )
    except ForwardDailyError as exc:
        value = {
            "schema_version": "quant.forward-daily-error/v1",
            "state": "configuration_failed",
            "code": exc.code,
            "reason": str(exc),
        }
        print(json.dumps(value, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return _exit_code(args.command, value)


if __name__ == "__main__":
    sys.exit(main())
