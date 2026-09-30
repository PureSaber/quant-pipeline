"""Frozen recipe promotion and append-only forward-paper observation, without brokers."""

from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import date, datetime, timezone
from html import escape
from pathlib import Path

import pandas as pd
from quant_lab.research import canonical, digest, file_hash, load_recipe, study_lock, verify_result
from quant_lab.trials import TrialRegistry

from quant_pipeline.research_validation import return_metrics
from quant_pipeline.research_workbench import equity_code_identity, input_identity


def _now(now):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError("Timezone-aware current time is required")
    return pd.Timestamp(now).tz_convert("Asia/Shanghai").to_pydatetime()


def promote(
    study: Path,
    candidate_name: str,
    output: Path,
    *,
    account_id: str,
    start: str,
    end: str,
    now=None,
) -> dict:
    from quant_report_hub.research_workbench import load_study

    from quant_pipeline.research_web import identifier

    now = _now(now)
    date.fromisoformat(start)
    date.fromisoformat(end)
    identifier(account_id)
    summary = load_study(study)
    recipe = deepcopy(summary["recipe"])
    if recipe["backend"] != "equity":
        raise ValueError("Forward accounts require the equity backend")
    pd.Timestamp(start)
    pd.Timestamp(end)
    chosen = next((r for r in summary["results"] if r["candidate"]["name"] == candidate_name), None)
    if any(row["status"] != "completed" for row in summary["results"]):
        raise ValueError("Complete the entire preregistered candidate family before promotion")
    if chosen is None or chosen["status"] != "completed":
        raise ValueError("Promote a completed preregistered candidate")
    if recipe.get("validation", {}).get("direction_policy") == "train_ic":
        raise ValueError(
            "Freeze explicit factor directions in a new study before forward promotion"
        )
    if output.exists():
        if not (output / "account.db").is_file():
            raise FileExistsError(output)
        with study_lock(output):
            record = TrialRegistry(output / "account.db").definition(account_id)
            frozen = record["definition"]
            requested = {
                "recipe": recipe,
                "parameters": [chosen["candidate"]],
                "code_identity": chosen["identity"],
                "holdout_start": start,
                "holdout_end": end,
                "source_study_sha256": file_hash(study),
                "source_definition": summary["definition_sha256"],
            }
            if any(frozen.get(key) != value for key, value in requested.items()):
                raise ValueError("Existing account registration differs from promotion request")
            if (
                input_lineage(recipe["inputs"]) != frozen["input_lineage"]
                or input_prefix(recipe["inputs"], frozen["initial_input_cutoff"])
                != frozen["initial_input_prefix"]
            ):
                raise ValueError("Existing account registration inputs changed")
            return publish_account(output, account_id, record)
    if chosen["identity"] != equity_code_identity():
        raise ValueError("Source study code differs from current runtime")
    if input_identity(recipe) != chosen.get("data_identity"):
        raise ValueError("Source study input snapshot changed before promotion")
    if start <= now.date().isoformat() or start <= recipe["interval"]["end"] or end <= start:
        raise ValueError(
            "Observation must start in the future after development and have positive duration"
        )
    local_now = pd.Timestamp(now).tz_convert("Asia/Shanghai")
    initial_cutoff = local_now.normalize()
    if local_now.hour < 15:
        initial_cutoff -= pd.Timedelta(days=1)
    initial_cutoff = str(initial_cutoff.date())
    from a_share_multifactor.decision_workflow import load_inputs

    _, frames = load_inputs(Path(recipe["inputs"]["bundle"]))
    calendar = pd.DatetimeIndex(pd.to_datetime(frames["calendar"].date))
    if pd.Timestamp(start) not in calendar:
        raise ValueError("Forward start must be an explicitly known exchange session")
    known = calendar[calendar <= initial_cutoff]
    if len(known) == 0:
        raise ValueError("Calendar cannot establish the latest completed session")
    latest = known.max()
    initial_cutoff = str(latest.date())
    expected_symbols = set(frames["raw"].symbol.astype(str))
    execution = recipe.get("execution")
    if execution is not None:
        from quant_data_kit.research_coverage import asof_history, load_history

        _, history = load_history(Path(recipe["inputs"]["history"]))
        close = latest.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
        for key, required in (("listed", "true"), ("delisted", "false"), ("tradable", "true")):
            rows = asof_history(
                history, as_of=close, domain="status", field=execution["status_fields"][key]
            )
            if not expected_symbols <= set(rows.symbol):
                raise ValueError("Current trading-state history is incomplete for promotion")
            expected_symbols &= set(rows.loc[rows.value.eq(required), "symbol"])
    for key in ("raw", "adjusted"):
        available = set(
            frames[key].loc[pd.to_datetime(frames[key].date).eq(latest), "symbol"].astype(str)
        )
        if not expected_symbols <= available:
            raise ValueError(
                "Source study snapshot is stale; rerun the study on a current snapshot"
            )
    if latest not in set(pd.to_datetime(frames["benchmark"].date)):
        raise ValueError("Source study benchmark is stale")
    definition = {
        "hypothesis": recipe["hypothesis"],
        "parameters": [chosen["candidate"]],
        "code_identity": chosen["identity"],
        "selection_rule": "one frozen candidate; no reselection",
        "recipe": recipe,
        "holdout_start": start,
        "holdout_end": end,
        "source_study_sha256": file_hash(study),
        "source_definition": summary["definition_sha256"],
        "source_metrics": chosen["metrics"],
        "source_scope": chosen["scope"],
        "source_candidate_statuses": [
            {"candidate": row["candidate"]["name"], "status": row["status"]}
            for row in summary["results"]
        ],
        "initial_input_cutoff": initial_cutoff,
        "initial_input_prefix": input_prefix(recipe["inputs"], initial_cutoff),
        "input_lineage": input_lineage(recipe["inputs"]),
    }
    output.mkdir(parents=True, exist_ok=False)
    registry = TrialRegistry(output / "account.db")
    registry.register(account_id, definition, now=now)
    return publish_account(output, account_id, registry.definition(account_id))


def publish_account(output: Path, account_id: str, record: dict) -> dict:
    account = {
        "schema_version": "quant.research-paper/v1",
        "account_id": account_id,
        "definition_sha256": record["sha256"],
        "definition": record["definition"],
        "created_at": record["registered_at"],
    }
    atomic_json(output / "account.json", account)
    write_account_report(output)
    return account


def load_account(root: Path) -> dict:
    account = json.loads((root / "account.json").read_text(encoding="utf-8"))
    if account.get("schema_version") != "quant.research-paper/v1":
        raise ValueError("Unsupported paper account")
    record = TrialRegistry(root / "account.db").definition(account["account_id"])
    if (
        record["sha256"] != account["definition_sha256"]
        or record["definition"] != account["definition"]
    ):
        raise ValueError("Paper account differs from frozen registration")
    return account


def _instrument_master_prefix(
    inputs: dict,
    cutoff: str,
    manifest: dict,
) -> tuple[dict | None, set[str]]:
    """Return a semantic prefix only for a fully verified QDK master projection."""
    from a_share_multifactor.run_contract import _canonical_frame_sha256
    from quant_data_kit.instrument_master import (
        BUNDLE_SCHEMA,
        instrument_master_prefix,
        load_instrument_master,
        resolve_instrument_catalog,
    )

    files = manifest.get("files", {})
    master_frames = {
        name
        for name, item in files.items()
        if isinstance(item, dict) and item.get("provider") == BUNDLE_SCHEMA
    }
    if not master_frames:
        return None, set()
    if master_frames != {"catalog"} or manifest.get("schema_version") != "qdk.research-dataset/v1":
        raise ValueError("Unsupported verified instrument-master projection")

    snapshot_identity = {
        key: value
        for key, value in manifest.items()
        if key not in {"snapshot_id", "identity_sha256"}
    }
    snapshot_sha256 = digest(snapshot_identity)
    if (
        manifest.get("identity_sha256") != snapshot_sha256
        or manifest.get("snapshot_id") != f"sha256-{snapshot_sha256}"
    ):
        raise ValueError("Instrument-master dataset snapshot identity changed")

    bundle = Path(inputs["bundle"]).resolve()
    item = files["catalog"]
    projected_catalog = (bundle / item["file"]).resolve()
    try:
        projected_catalog.relative_to(bundle)
    except ValueError as exc:
        raise ValueError("Instrument-master catalog projection escapes the bundle") from exc
    if projected_catalog != Path(inputs["catalog"]).resolve():
        raise ValueError("Recipe catalog is not the verified instrument-master projection")
    if file_hash(projected_catalog) != item.get("sha256"):
        raise ValueError("Instrument-master catalog projection hash changed")

    master_root = bundle / "instrument_master"
    master_manifest, versioned_catalog = load_instrument_master(master_root)
    validation = manifest.get("validation", {}).get("instrument_master", {})
    if (
        validation.get("passed") is not True
        or validation.get("bundle_sha256") != master_manifest["bundle_sha256"]
    ):
        raise ValueError("Dataset does not bind the verified instrument-master identity")
    expected = resolve_instrument_catalog(
        versioned_catalog,
        symbols=[str(symbol) for symbol in manifest["symbols"]],
        start=pd.Timestamp(manifest["requested_start"]),
        end=pd.Timestamp(manifest["requested_end"]),
    )
    actual = pd.read_csv(projected_catalog, dtype=str, keep_default_na=False)
    if _canonical_frame_sha256(actual) != _canonical_frame_sha256(expected):
        raise ValueError("Instrument-master catalog projection differs from verified evidence")

    cutoff_close = pd.Timestamp(cutoff).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
    prefix = instrument_master_prefix(master_root, cutoff=cutoff_close)
    return {
        "kind": "verified-instrument-master-prefix",
        "value": prefix,
    }, master_frames


def input_prefix(inputs: dict, cutoff: str) -> dict:
    """Hash all observations known on/before cutoff to reject historical revisions."""
    from a_share_multifactor.decision_workflow import load_inputs
    from a_share_multifactor.run_contract import _canonical_frame_sha256
    from quant_data_kit.research_coverage import load_history

    manifest, frames = load_inputs(Path(inputs["bundle"]))
    master_prefix, master_frames = _instrument_master_prefix(inputs, cutoff, manifest)
    identity = {}
    for name, frame in frames.items():
        if name in master_frames:
            identity[name] = master_prefix
            continue
        if "date" in frame:
            frame = frame[pd.to_datetime(frame.date) <= pd.Timestamp(cutoff)]
        elif "available_at" in frame:
            close = pd.Timestamp(cutoff).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
            frame = frame[pd.to_datetime(frame.available_at, utc=True) <= close]
        elif "announced_date" in frame:
            frame = frame[pd.to_datetime(frame.announced_date) <= pd.Timestamp(cutoff)]
        identity[name] = _canonical_frame_sha256(frame)
    identity["catalog"] = master_prefix or file_hash(Path(inputs["catalog"]))
    if "history" in inputs:
        _, history = load_history(Path(inputs["history"]))
        close = pd.Timestamp(cutoff).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
        history = history[history.available_at <= close]
        identity["history"] = _canonical_frame_sha256(history)
    return identity


def input_lineage(inputs: dict) -> dict:
    """Keep provider, symbol universe and snapshot storage family fixed on promotion."""
    from a_share_multifactor.decision_workflow import load_inputs

    root = Path(inputs["bundle"]).resolve()
    manifest, frames = load_inputs(root)
    source = manifest.get("source", {})
    if not isinstance(source, dict):
        source = {"declaration": source}
    lineage = {
        "bundle_parent": str(root.parent),
        "origin": manifest.get("origin"),
        "source": {
            key: source[key]
            for key in ("mode", "provider", "endpoints", "source_uri", "declaration")
            if key in source
        },
        "providers": {key: item.get("provider") for key, item in manifest["files"].items()},
        "symbols": sorted(frames["raw"].symbol.astype(str).unique().tolist()),
        "input_source": inputs.get("source"),
    }
    if "history" in inputs:
        from quant_data_kit.research_coverage import load_history

        history, _ = load_history(Path(inputs["history"]))
        lineage["history"] = {
            key: history.get(key)
            for key in (
                "schema_version",
                "provider",
                "source_uri",
                "license_note",
                "domains",
                "symbols",
            )
        }
    if "config" in inputs:
        lineage["config_sha256"] = file_hash(Path(inputs["config"]))
    return lineage


def observations(root, account):
    registry = TrialRegistry(root / "account.db")
    completed = []
    for event in registry.history(account["account_id"]):
        if event["status"] == "completed":
            payload = event["payload"]
            path = (root / payload["result"]).resolve()
            if root.resolve() not in path.parents:
                raise ValueError("Observation escapes account")
            completed.append((verify_result(path, payload["sha256"]), path.parent))
    return completed


def sealed_evidence(root: Path, account_id: str):
    with TrialRegistry(root / "account.db").connect() as db:
        row = db.execute(
            "SELECT evidence FROM holdout_evaluations WHERE study_id=?", (account_id,)
        ).fetchone()
    return json.loads(row[0]) if row else None


def atomic_json(path: Path, value: dict):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(canonical(value), encoding="utf-8")
    temporary.replace(path)


def publish_observation(root: Path, result: dict, directory: Path):
    path = directory / "result.json"
    atomic_json(
        root / "latest.json",
        {
            "as_of": result["as_of"],
            "result": str(path.relative_to(root)),
            "sha256": file_hash(path),
        },
    )
    write_account_report(root)


def observe(root: Path, inputs: dict, *, as_of: str, now=None, executor=None) -> dict:
    from a_share_multifactor.decision_workflow import load_inputs
    from a_share_multifactor.research_workbench import EquityResearchExecutor

    root = root.resolve()
    now = _now(now)
    date.fromisoformat(as_of)
    with study_lock(root):
        account = load_account(root)
        spec = account["definition"]
        close = pd.Timestamp(as_of).tz_localize("Asia/Shanghai") + pd.Timedelta(hours=15)
        if close.to_pydatetime() > now or not spec["holdout_start"] <= as_of <= spec["holdout_end"]:
            raise ValueError(
                "Observation requires a completed session within the registered interval"
            )
        if equity_code_identity() != spec["code_identity"]:
            raise ValueError("Forward account code identity changed")
        if set(inputs) != set(spec["recipe"]["inputs"]):
            raise ValueError("Forward input domains differ from frozen recipe")
        if input_lineage(inputs) != spec["input_lineage"]:
            raise ValueError("Forward input lineage differs from the promoted dataset")
        if input_prefix(inputs, spec["initial_input_cutoff"]) != spec["initial_input_prefix"]:
            raise ValueError("Historical inputs were revised after promotion")
        evidence = input_identity({"inputs": inputs})
        registry = TrialRegistry(root / "account.db")
        with registry.connect() as db:
            if db.execute(
                "SELECT 1 FROM holdout_evaluations WHERE study_id=?", (account["account_id"],)
            ).fetchone():
                raise ValueError("Forward account is sealed")
        previous = observations(root, account)
        if previous:
            last, directory = previous[-1]
            if as_of < last["as_of"]:
                raise ValueError("Observations must advance monotonically")
            if input_prefix(inputs, last["as_of"]) != last["input_prefix"]:
                raise ValueError(
                    "Historical inputs were revised; forward account cannot be rewritten"
                )
            if as_of == last["as_of"]:
                publish_observation(root, last, directory)
                return last
        else:
            directory = None
        _, frames = load_inputs(Path(inputs["bundle"]))
        calendar = pd.DatetimeIndex(pd.to_datetime(frames["calendar"].date))
        expected = calendar[(calendar >= spec["holdout_start"]) & (calendar <= as_of)]
        if (
            len(expected) == 0
            or str(expected[0].date()) != spec["holdout_start"]
            or str(expected[-1].date()) != as_of
        ):
            raise ValueError("Observation endpoints must be exchange sessions")
        history = registry.history(account["account_id"])
        terminals = {e["attempt_id"] for e in history if e["status"] != "running"}
        for event in history:
            if event["status"] == "running" and event["attempt_id"] not in terminals:
                registry.finish(
                    event["attempt_id"], "interrupted", {"reason": "exclusive writer recovered"}
                )
        candidate = spec["parameters"][0]
        attempt = registry.start(
            account["account_id"], candidate, context={"as_of": as_of, "inputs": evidence}
        )
        out = root / "observations" / attempt
        out.mkdir(parents=True)
        terminal = False
        try:
            recipe = deepcopy(spec["recipe"])
            recipe.pop("validation", None)
            recipe["inputs"] = inputs
            recipe["interval"] = {"start": spec["holdout_start"], "end": as_of}
            result = (executor or EquityResearchExecutor())(recipe, candidate, out)
            returns = pd.read_csv(out / "returns.csv", index_col=0, parse_dates=True).iloc[:, 0]
            if not returns.index.equals(expected):
                raise ValueError("Forward result lacks exchange sessions")
            if directory:
                old = pd.read_csv(directory / "returns.csv", index_col=0, parse_dates=True).iloc[
                    :, 0
                ]
                if not returns.reindex(old.index).equals(old):
                    raise ValueError("Deterministic replay changed previous account returns")
            result.update(
                account_id=account["account_id"],
                as_of=as_of,
                status="observed",
                inputs=inputs,
                input_identity=evidence,
                input_prefix=input_prefix(inputs, as_of),
                account_policy="continuous account replay from frozen start; no daily capital reset",
                observed_at=now.isoformat(),
            )
            result["diagnostics"] = []
            maximum = spec["recipe"].get("risk", {}).get("max_drawdown")
            if maximum is not None and abs(result["metrics"].get("max_drawdown", 0)) >= maximum:
                result["diagnostics"].append(
                    {
                        "code": "DRAWDOWN_LIMIT",
                        "message": "模拟账户回撤达到配方限制，需检查风控及策略状态。",
                    }
                )
            if "synthetic" in spec["source_scope"]:
                result["diagnostics"].append(
                    {
                        "code": "SYNTHETIC_ONLY",
                        "message": "该账户源自合成数据研究，只用于软件验证。",
                    }
                )
            result["artifacts"] = {
                str(p.relative_to(out)).replace("\\", "/"): file_hash(p)
                for p in sorted(out.rglob("*"))
                if p.is_file()
            }
            path = out / "result.json"
            path.write_text(canonical(result), encoding="utf-8")
            registry.finish(
                attempt,
                "completed",
                {"result": str(path.relative_to(root)), "sha256": file_hash(path)},
            )
            terminal = True
            publish_observation(root, result, out)
            return result
        except Exception as exc:
            if not terminal:
                registry.finish(
                    attempt,
                    "failed",
                    {"as_of": as_of, "error": str(exc), "error_type": type(exc).__name__},
                )
                write_account_report(root)
            raise


def seal(root: Path, *, now=None) -> dict:
    now = _now(now)
    with study_lock(root):
        account = load_account(root)
        spec = account["definition"]
        if now.date().isoformat() <= spec["holdout_end"]:
            raise ValueError("Forward observation has not ended")
        completed = observations(root, account)
        if not completed:
            raise ValueError("No completed forward observations")
        last, directory = completed[-1]
        if input_identity({"inputs": last["inputs"]}) != last["input_identity"]:
            raise ValueError("Final forward snapshot changed before sealing")
        from a_share_multifactor.decision_workflow import load_inputs

        _, frames = load_inputs(Path(last["inputs"]["bundle"]))
        dates = pd.DatetimeIndex(pd.to_datetime(frames["calendar"].date))
        if dates.max() < pd.Timestamp(spec["holdout_end"]):
            raise ValueError("Calendar does not cover the registered end")
        expected = dates[(dates >= spec["holdout_start"]) & (dates <= spec["holdout_end"])]
        returns = pd.read_csv(directory / "returns.csv", index_col=0, parse_dates=True).iloc[:, 0]
        if not returns.index.equals(expected):
            raise ValueError("Final observation does not cover every registered session")
        evidence = {
            "start": spec["holdout_start"],
            "end": spec["holdout_end"],
            "code_identity": spec["code_identity"],
            "input_sha256": digest(last["input_identity"]),
            "strategy_net": return_metrics(returns),
            "source_research_metrics": spec["source_metrics"],
            "scope": "synthetic-forward-software-validation"
            if "synthetic" in spec["source_scope"]
            else "single-preregistered-forward-observation",
            "limitations": [
                "historical and forward intervals differ; metrics are not a paired performance claim"
            ],
        }
        existing = sealed_evidence(root, account["account_id"])
        if existing is None:
            TrialRegistry(root / "account.db").seal_holdout(
                account["account_id"], evidence, now=now
            )
        elif existing != evidence:
            raise ValueError("Sealed evidence differs; forward accounts cannot be reevaluated")
        atomic_json(root / "evaluation.json", evidence)
        write_account_report(root)
        return evidence


def write_account_report(root):
    account = load_account(root)
    registry = TrialRegistry(root / "account.db")
    completed = observations(root, account)
    report = {
        "account_id": account["account_id"],
        "definition": account["definition"],
        "attempts": registry.history(account["account_id"]),
        "observations": [
            {"as_of": r["as_of"], "metrics": r["metrics"], "diagnostics": r.get("diagnostics", [])}
            for r, _ in completed
        ],
    }
    evaluation = sealed_evidence(root, account["account_id"])
    if evaluation is not None:
        report["evaluation"] = evaluation
    page = (
        '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>前向模拟观察</title><style>body{font:16px system-ui;max-width:1000px;margin:40px auto}pre{white-space:pre-wrap;word-break:break-word;background:#f1f5f6;padding:24px}</style><h1>前向模拟观察</h1><p>参数和代码已冻结；账本从注册起点连续重放。日常指标用于运行检查，观察结束后才生成一次性评价。</p><pre>'
        + escape(canonical(report))
        + "</pre></html>"
    )
    temporary = root / "account.html.tmp"
    temporary.write_text(page, encoding="utf-8")
    temporary.replace(root / "account.html")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("promote")
    create.add_argument("study", type=Path)
    create.add_argument("--candidate", default="base")
    create.add_argument("--output", type=Path, required=True)
    create.add_argument("--account-id", required=True)
    create.add_argument("--start", required=True)
    create.add_argument("--end", required=True)
    update = sub.add_parser("observe")
    update.add_argument("account", type=Path)
    update.add_argument(
        "--recipe", type=Path, required=True, help="Read only new input references from this recipe"
    )
    update.add_argument("--as-of", required=True)
    finish = sub.add_parser("seal")
    finish.add_argument("account", type=Path)
    report = sub.add_parser("report")
    report.add_argument("account", type=Path)
    args = parser.parse_args()
    if args.command == "promote":
        value = promote(
            args.study,
            args.candidate,
            args.output,
            account_id=args.account_id,
            start=args.start,
            end=args.end,
        )
    elif args.command == "observe":
        value = observe(args.account, load_recipe(args.recipe)["inputs"], as_of=args.as_of)
    elif args.command == "seal":
        value = seal(args.account)
    else:
        value = write_account_report(args.account)
    print(canonical(value))


if __name__ == "__main__":
    main()
