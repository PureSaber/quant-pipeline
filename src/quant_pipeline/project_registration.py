"""Project-bound preview/confirm adapter; never schedules or observes an account."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from quant_lab.research import digest, file_hash, study_lock
from quant_lab.trials import TrialRegistry

from quant_pipeline import research_paper as paper

SCHEMA = "quant.project-registration/v1"


def _read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _child(root, name):
    path = (root / name).resolve()
    if root.resolve() not in path.parents:
        raise ValueError("Registration path escapes configured root")
    return path


def _id(value, pattern=r"[a-f0-9]{32}"):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError("Invalid registration identifier")
    return value


class Registration:
    def __init__(self, config):
        self.path = Path(config).resolve()
        self.config = _read(self.path)
        if (
            set(self.config)
            != {"schema", "runs_root", "accounts_root", "state_root", "preview_ttl_seconds"}
            or self.config["schema"] != SCHEMA
        ):
            raise ValueError("Invalid server-owned registration configuration")
        ttl = self.config["preview_ttl_seconds"]
        if type(ttl) is not int or not 60 <= ttl <= 3600:
            raise ValueError("Preview TTL must be 60 to 3600 seconds")
        roots = []
        for key in ("runs_root", "accounts_root", "state_root"):
            path = Path(self.config[key])
            if not path.is_absolute():
                raise ValueError("Registration roots must be absolute")
            roots.append(path.resolve())
        for i, left in enumerate(roots):
            for right in roots[i + 1 :]:
                if left == right or left in right.parents or right in left.parents:
                    raise ValueError("Registration roots must be separate")
        self.runs, self.accounts, self.state = roots

    def _project(self, project_id, revision=None):
        project_id = _id(project_id)
        directory = _child(self.runs, ".projects/" + project_id)
        head = _id((directory / "HEAD").read_text().strip(), r"[a-f0-9]{64}")
        if revision is not None and head != revision:
            raise ValueError("STALE_PREVIEW: project version changed; preview again")
        project = _read(_child(directory, head + ".json"))
        content = {k: v for k, v in project.items() if k != "revision"}
        sha = hashlib.sha256(
            json.dumps(content, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
        ).hexdigest()
        if project.get("id") != project_id or project.get("revision") != head or sha != head:
            raise ValueError("Project identity verification failed")
        return project

    def _source(self, request):
        if set(request) != {"project_id", "revision", "run_id", "candidate", "start", "end"}:
            raise ValueError("Unexpected registration fields")
        project = self._project(request["project_id"], request["revision"])
        if not project.get("research_card"):
            raise ValueError("Project research card is required")
        run = _child(self.runs, _id(request["run_id"], r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))
        saved = _read(run / "request.json")
        binding = saved.get("research_project") or {}
        if binding.get("id") != project["id"]:
            raise ValueError("Choose an explicitly bound experiment from this project")
        # The experiment may use an earlier project revision; its immutable recipe must
        # still be connected to the currently reviewed project.
        recipe_ref = saved.get("recipe") or {}
        if {"id": recipe_ref.get("id"), "revision": recipe_ref.get("revision")} not in project[
            "recipes"
        ]:
            raise ValueError("Experiment recipe version is no longer connected to this project")
        result = _read(run / "result.json")
        if saved.get("template_id") != "etf-weekly-research" or result.get("status") != "succeeded":
            raise ValueError("Choose a completed native ETF experiment")
        if result.get("verification_status") == "failed":
            raise ValueError("Experiment verification failed")
        hashes = result.get("output_hashes") or {}
        for required in (
            "request.json",
            "strategy-output/study.json",
            "strategy-output/experiments.db",
        ):
            if required not in hashes:
                raise ValueError("Experiment lacks a frozen native artifact manifest")
        for name, expected in hashes.items():
            if file_hash(_child(run, name)) != expected:
                raise ValueError("Experiment artifact changed: " + name)
        return run / "strategy-output/study.json", digest(hashes)

    def _plan(self, request, now):
        from a_share_multifactor.research_workbench import preflight_recipe

        study, artifacts = self._source(request)
        definition = paper.prepare_promotion(
            study,
            request["candidate"],
            account_id="preview",
            start=request["start"],
            end=request["end"],
            now=now,
            require_timely_orders=True,
        )
        preflight = preflight_recipe(definition["recipe"])
        if preflight.get("passed") is not True:
            raise ValueError(
                "Research prerequisites failed: " + json.dumps(preflight, ensure_ascii=False)
            )
        return {
            "definition": definition,
            "artifacts_sha256": artifacts,
            "config_sha256": file_hash(self.path),
            "prerequisites": preflight,
            "study": str(study),
        }

    def preview(self, request, *, now=None):
        now = paper._now(now)
        plan = self._plan(request, now)
        # Exclude diagnostic timestamps from the commit identity.
        identity = {
            k: plan[k] for k in ("definition", "artifacts_sha256", "config_sha256", "study")
        }
        account_id = "sim-" + digest({"request": request, "identity": identity})[:32]
        preview = {
            "schema": SCHEMA,
            "id": uuid.uuid4().hex,
            "request": request,
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=self.config["preview_ttl_seconds"])).isoformat(),
            "plan": plan,
            "identity": identity,
            "account_id": account_id,
            "state": "prepared",
            "scheduled": False,
        }
        self.state.mkdir(parents=True, exist_ok=True)
        preview["sha256"] = digest(preview)
        paper.atomic_json(_child(self.state, preview["id"] + ".json"), preview)
        return preview

    def get(self, project_id, preview_id):
        self._project(project_id)
        preview = _read(_child(self.state, _id(preview_id) + ".json"))
        if preview.get("sha256") != digest({k: v for k, v in preview.items() if k != "sha256"}):
            raise ValueError("Preview integrity verification failed")
        if preview["request"]["project_id"] != project_id or preview.get("schema") != SCHEMA:
            raise ValueError("Preview does not belong to this project")
        receipt_path = _child(self.state, preview["account_id"] + ".json")
        receipt = _read(receipt_path) if receipt_path.exists() else None
        result = {**preview, "receipt": receipt}
        target = _child(self.accounts, preview["account_id"])
        if target.exists():
            try:
                result["inspection"] = self._verify(preview, target, paper._now(None))
            except Exception as exc:  # noqa: BLE001 - expose native inspection failure
                result["inspection_error"] = str(exc)
        return result

    def list(self, project_id):
        self._project(project_id)
        previews = []
        for path in self.state.glob("*.json"):
            if re.fullmatch(r"[a-f0-9]{32}", path.stem):
                value = _read(path)
                if value.get("request", {}).get("project_id") == project_id:
                    previews.append(
                        {k: value[k] for k in ("id", "account_id", "created_at", "expires_at")}
                    )
        return {
            "schema": SCHEMA,
            "previews": sorted(previews, key=lambda row: row["created_at"], reverse=True),
        }

    def _verify(self, preview, root, now):
        account = paper.load_account(root)
        if (
            account["account_id"] != preview["account_id"]
            or account["definition"] != preview["plan"]["definition"]
        ):
            raise ValueError("Registered account differs from confirmed preview")
        return paper.inspect_account(root, now=now)

    def commit(self, project_id, preview_id, *, now=None):
        now = paper._now(now)
        with study_lock(self.state):
            preview = self.get(project_id, preview_id)
            account_id = preview["account_id"]
            target = _child(self.accounts, account_id)
            stage = _child(self.accounts, ".pending-" + account_id)
            receipt_path = _child(self.state, account_id + ".json")
            receipt = preview["receipt"] or {
                "schema": SCHEMA,
                "account_id": account_id,
                "preview_id": preview_id,
                "scheduled": False,
                "attempts": [],
            }
            # A committed account can be inspected/recovered after preview expiry;
            # this path never creates a new account or changes its frozen definition.
            if target.exists():
                inspection = self._verify(preview, target, now)
                receipt.update(state="completed", inspection=inspection, error=None)
                paper.atomic_json(receipt_path, receipt)
                return receipt
            if stage.exists() and receipt["attempts"]:
                try:
                    record = TrialRegistry(stage / "account.db", read_only=True).definition(
                        account_id
                    )
                except (ValueError, KeyError, OSError, sqlite3.Error):
                    record = None
                if record is not None:
                    if record["definition"] != preview["plan"]["definition"]:
                        raise ValueError("Interrupted registration definition differs")
                    paper.publish_account(stage, account_id, record)
                    inspection = self._verify(preview, stage, now)
                    stage.rename(target)
                    receipt.update(state="completed", inspection=inspection, error=None)
                    paper.atomic_json(receipt_path, receipt)
                    return receipt
            if now >= datetime.fromisoformat(preview["expires_at"]):
                raise ValueError("STALE_PREVIEW: preview expired; preview again")
            plan = self._plan(preview["request"], now)
            if any(plan[key] != value for key, value in preview["identity"].items()):
                raise ValueError("STALE_PREVIEW: source, input, runtime or configuration changed")
            self.accounts.mkdir(parents=True, exist_ok=True)
            if stage.exists():
                # Preserve the failed database for diagnosis, never erase evidence.
                stage.rename(
                    _child(self.accounts, ".failed-" + account_id + "-" + uuid.uuid4().hex)
                )
            receipt["attempts"].append({"started_at": now.isoformat(), "state": "registering"})
            receipt.update(state="registering", error=None)
            paper.atomic_json(receipt_path, receipt)
            try:
                paper.promote(
                    Path(plan["study"]),
                    preview["request"]["candidate"],
                    stage,
                    account_id=account_id,
                    start=preview["request"]["start"],
                    end=preview["request"]["end"],
                    now=now,
                    require_timely_orders=True,
                )
                inspection = self._verify(preview, stage, now)
                stage.rename(target)
                receipt.update(state="completed", inspection=inspection, error=None)
                receipt["attempts"][-1]["state"] = "completed"
            except Exception as exc:  # noqa: BLE001 - persist all native failure evidence
                receipt.update(state="recoverable_failure", error=str(exc))
                receipt["attempts"][-1].update(state="failed", error=str(exc))
            paper.atomic_json(receipt_path, receipt)
            return receipt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preview", "commit", "inspect", "list"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--request-json")
    args = parser.parse_args(argv)
    try:
        request = json.loads(args.request_json or sys.stdin.read(65537))
        service = Registration(args.config)
        if args.command == "preview":
            value = service.preview(request)
        elif args.command == "list":
            if set(request) != {"project_id"}:
                raise ValueError("Unexpected listing fields")
            value = service.list(request["project_id"])
        else:
            if set(request) != {"project_id", "preview_id"}:
                raise ValueError("Unexpected confirmation fields")
            method = service.commit if args.command == "commit" else service.get
            value = method(request["project_id"], request["preview_id"])
        print(json.dumps(value, ensure_ascii=False, allow_nan=False))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI returns structured native diagnostics
        print(
            json.dumps(
                {"schema": SCHEMA, "state": "blocked", "error": str(exc)}, ensure_ascii=False
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
