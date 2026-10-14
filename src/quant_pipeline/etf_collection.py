"""Killable acquisition worker. Never executes a strategy or opens an account."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

from quant_lab.research import canonical


class CollectionFailure(RuntimeError):
    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def retryable(exc, stage, as_of):
    from quant_pipeline.etf_daily import _publication_delay

    transport = isinstance(exc, (TimeoutError, ConnectionError)) or (
        type(exc).__module__.startswith(("requests", "urllib3"))
        and type(exc).__name__ in {"Timeout", "ConnectTimeout", "ReadTimeout", "ConnectionError"}
    )
    return (stage in {"master_refresh", "collection"} and transport) or (
        stage == "collection" and _publication_delay(exc, as_of)
    )


def collect(profile, directory, as_of, stage, *, fetch=True):
    from quant_data_kit.research_dataset import bind_instrument_master, update_dataset

    from quant_pipeline.etf_daily import _identity_unchanged

    _identity_unchanged(profile)
    if profile.get("master_refresh"):
        from quant_pipeline.etf_master_refresh import refresh

        stage("master_refresh", "running")
        bundle, evidence = refresh(profile["master_refresh"], directory / "master-refresh")
        _identity_unchanged(profile)
        bind_instrument_master(profile["dataset_root"], bundle)
        stage("master_refresh", "completed", evidence=evidence)
    stage("collection", "running")
    if fetch:
        update_dataset(profile["dataset_root"], end=as_of, overlap_sessions=5)


def bounded(profile, directory, as_of, stage, *, timeout, fetch=True):
    command = [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "quant_pipeline.etf_collection",
        "--profile",
        str(profile["profile_path"]),
        "--directory",
        str(directory),
        "--as-of",
        as_of,
    ]
    if not fetch:
        command.append("--no-fetch")
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        progress = directory / "collection-progress.json"
        saved = json.loads(progress.read_text(encoding="utf-8")) if progress.exists() else []
        for row in saved:
            stage(**row)
        current = saved[-1]["name"] if saved else "worker_start"
        raise CollectionFailure(
            "Collection exceeded its time budget; worker stopped; partial evidence retained",
            retryable=current in {"master_refresh", "collection"},
        ) from exc
    (directory / "collection-stderr.txt").write_text(result.stderr, encoding="utf-8")
    try:
        value = json.loads((directory / "collection-result.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CollectionFailure("Collection worker failed without a valid receipt") from exc
    for row in value["stages"]:
        stage(**row)
    if result.returncode or value["state"] != "collected":
        raise CollectionFailure(
            value.get("error", "Collection failed"), retryable=value.get("retryable") is True
        )


def main():
    from quant_pipeline.etf_daily import load_profile
    from quant_pipeline.research_paper import atomic_json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--no-fetch", action="store_true")
    args = parser.parse_args()
    stages = []

    def stage(name, state, **evidence):
        stages.append({"name": name, "state": state, **evidence})
        atomic_json(args.directory / "collection-progress.json", stages)

    value = {"state": "collected", "stages": stages}
    try:
        collect(
            load_profile(args.profile), args.directory, args.as_of, stage, fetch=not args.no_fetch
        )
    except Exception as exc:  # noqa: BLE001 - preserve provider errors for the parent
        value.update(
            state="failed",
            error=str(exc),
            error_type=type(exc).__name__,
            retryable=retryable(exc, stages[-1]["name"] if stages else "", args.as_of),
        )
    atomic_json(args.directory / "collection-result.json", value)
    print(canonical(value))
    return 0 if value["state"] == "collected" else 3


if __name__ == "__main__":
    raise SystemExit(main())
