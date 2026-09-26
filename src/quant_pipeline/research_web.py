"""Loopback-only research console backed by the existing recipe and study contracts."""

from __future__ import annotations

import argparse
import hmac
import json
import mimetypes
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from functools import wraps
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

import yaml
from quant_lab.research import (
    candidates,
    canonical,
    compare_results,
    digest,
    load_recipe,
    study_lock,
    validate_recipe,
)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value):
        raise ValueError("Invalid identifier")
    return value


def safe_child(root: Path, *parts: str) -> Path:
    """Managed outputs cannot traverse or follow a junction/symlink, even inside root."""
    root = root.absolute()
    expected = root.joinpath(*parts).absolute()
    resolved = expected.resolve()
    if root.resolve() != root or resolved != expected or root not in resolved.parents:
        raise ValueError("Managed path escapes its root or uses a filesystem link")
    return resolved


def mutation(method):
    @wraps(method)
    def synchronized(self, *args, **kwargs):
        with self.mutation_lock:
            return method(self, *args, **kwargs)

    return synchronized


class ResearchConsole:
    def __init__(self, root: Path, data_roots: list[Path], *, start_worker=True):
        self.root = root.resolve()
        self.data_roots = [p.resolve() for p in data_roots]
        self.root.mkdir(parents=True, exist_ok=True)
        self.exclusive = study_lock(safe_child(self.root, ".console"))
        self.exclusive.__enter__()
        self.mutation_lock = threading.RLock()
        for name in ("recipes", "runs", "logs", "accounts", "requests"):
            safe_child(self.root, name).mkdir(exist_ok=True)
        self.database = safe_child(self.root, "console.db")
        self.token = secrets.token_urlsafe(32)
        self.wakeup = threading.Event()
        self.stopping = threading.Event()
        self.process = None
        self.worker = None
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, study_id TEXT UNIQUE NOT NULL, recipe TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    error TEXT);
                CREATE TABLE IF NOT EXISTS notes (
                    id INTEGER PRIMARY KEY, study_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, body TEXT NOT NULL);
            """)
            db.execute(
                "UPDATE jobs SET status='interrupted',error='Server restarted; resume explicitly',updated_at=? WHERE status='running'",
                (utcnow(),),
            )
        if start_worker:
            self.worker = threading.Thread(target=self._work, daemon=True)
            self.worker.start()

    def connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        return db

    def recipe_path(self, study_id):
        return self.managed("recipes", identifier(study_id) + ".yaml")

    def managed(self, area, *parts):
        directory = safe_child(self.root, area)
        return safe_child(directory, *parts) if parts else directory

    def validate_inputs(self, recipe):
        result = validate_recipe(recipe)
        for key, value in result["inputs"].items():
            if key == "source":
                continue
            target = Path(value)
            if not target.is_absolute():
                raise ValueError("Console input references must be absolute paths")
            target = target.resolve()
            if not any(target == root or root in target.parents for root in self.data_roots):
                raise ValueError("Input path lies outside configured data roots")
            if not target.exists():
                raise ValueError(f"Input reference does not exist: {key}")
        return result

    @mutation
    def save_recipe(self, recipe):
        recipe = self.validate_inputs(recipe)
        target = self.recipe_path(recipe["study_id"])
        with self.connect() as db:
            if db.execute("SELECT 1 FROM jobs WHERE study_id=?", (recipe["study_id"],)).fetchone():
                raise ValueError("A registered recipe is frozen; clone it with a new study_id")
        temporary = self.managed("recipes", target.stem + ".tmp")
        temporary.write_text(
            yaml.safe_dump(recipe, allow_unicode=True, sort_keys=False), encoding="utf-8"
        )
        temporary.replace(target)
        return recipe

    @mutation
    def clone(self, study_id, new_id):
        target = self.recipe_path(new_id)
        if target.exists():
            raise ValueError("Clone destination already exists")
        recipe = load_recipe(self.recipe_path(study_id))
        recipe["study_id"] = identifier(new_id)
        # A copied exploration must never inherit an untouched-holdout claim implicitly.
        recipe["mode"] = "exploratory"
        recipe.pop("holdout", None)
        return self.save_recipe(recipe)

    def preflight(self, study_id):
        recipe = self.validate_inputs(load_recipe(self.recipe_path(study_id)))
        planned = candidates(recipe)
        if recipe["backend"] != "equity":
            raise ValueError("The console runs equity/ETF recipes; frozen backends use the CLI")
        from a_share_multifactor.research_workbench import preflight_recipe

        reports = []
        for candidate in planned:
            reports.append({"candidate": candidate["name"], **preflight_recipe(recipe, candidate)})
        return {"passed": all(row["passed"] for row in reports), "candidates": reports}

    @mutation
    def submit(self, study_id):
        recipe = self.validate_inputs(load_recipe(self.recipe_path(study_id)))
        if recipe["backend"] != "equity":
            raise ValueError("Console execution supports equity/ETF only")
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM jobs WHERE study_id=?", (study_id,)).fetchone()
            if old:
                if json.loads(old["recipe"]) != recipe:
                    raise ValueError("Registered recipe changed; clone as a new study")
                if old["status"] in {"failed", "interrupted"}:
                    db.execute(
                        "UPDATE jobs SET status='queued',error=NULL,updated_at=? WHERE id=?",
                        (utcnow(), old["id"]),
                    )
                job_id = old["id"]
            else:
                job_id = uuid.uuid4().hex
                db.execute(
                    "INSERT INTO jobs VALUES(?,?,?,?,?,?,NULL)",
                    (job_id, study_id, canonical(recipe), "queued", utcnow(), utcnow()),
                )
        self.wakeup.set()
        return {"job_id": job_id}

    def job(self, job_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (identifier(job_id),)).fetchone()
        if row is None:
            raise ValueError("Unknown job")
        return dict(row)

    def state(self):
        with self.connect() as db:
            jobs = [
                dict(r)
                for r in db.execute(
                    "SELECT id,study_id,status,created_at,updated_at,error FROM jobs ORDER BY created_at DESC"
                )
            ]
        recipes = []
        for path in sorted(self.managed("recipes").glob("*.yaml")):
            try:
                path = self.managed("recipes", path.name)
                recipe = load_recipe(path)
                recipes.append(
                    {
                        "study_id": recipe["study_id"],
                        "hypothesis": recipe["hypothesis"],
                        "interval": recipe["interval"],
                        "validation": bool(recipe.get("validation")),
                    }
                )
            except (ValueError, OSError, yaml.YAMLError) as exc:
                recipes.append({"study_id": path.stem, "error": str(exc)})
        accounts = []
        from quant_pipeline.research_paper import load_account, sealed_evidence

        for path in sorted(self.managed("accounts").glob("*/account.json")):
            path = self.managed("accounts", path.parent.name, path.name)
            account = load_account(path.parent)
            accounts.append(
                {
                    "account_id": account["account_id"],
                    "start": account["definition"]["holdout_start"],
                    "end": account["definition"]["holdout_end"],
                    "sealed": sealed_evidence(path.parent, account["account_id"]) is not None,
                }
            )
        return {
            "recipes": recipes,
            "jobs": jobs,
            "accounts": accounts,
            "data_roots": [str(p) for p in self.data_roots],
        }

    def advice(self, query, study_id=None):
        from quant_agent.research_history import research_advice

        if not isinstance(query, str) or not 1 <= len(query.strip()) <= 10000:
            raise ValueError("A query of at most 10000 characters is required")
        recipe = load_recipe(self.recipe_path(study_id)) if study_id else None
        return research_advice(query, studies_root=self.managed("runs"), recipe=recipe)

    @mutation
    def promote(self, job_id, candidate, account_id, start, end):
        from quant_pipeline.research_paper import promote

        job = self.job(job_id)
        return promote(
            self.managed("runs", job["study_id"], "study.json"),
            candidate,
            self.managed("accounts", identifier(account_id)),
            account_id=account_id,
            start=start,
            end=end,
        )

    def observe(self, account_id, study_id, as_of):
        from quant_pipeline.research_paper import observe

        recipe = self.validate_inputs(load_recipe(self.recipe_path(study_id)))
        return observe(
            self.managed("accounts", identifier(account_id)), recipe["inputs"], as_of=as_of
        )

    def seal(self, account_id):
        from quant_pipeline.research_paper import seal

        return seal(self.managed("accounts", identifier(account_id)))

    def compare(self, job_ids):
        from quant_report_hub.research_workbench import load_study

        if (
            not isinstance(job_ids, list)
            or not 2 <= len(job_ids) <= 8
            or len(set(job_ids)) != len(job_ids)
        ):
            raise ValueError("Select two to eight distinct studies")
        summaries = []
        for job_id in job_ids:
            job = self.job(job_id)
            summaries.append(load_study(self.managed("runs", job["study_id"], "study.json")))
        results = [
            next(r for r in s["results"] if r["candidate"]["name"] == "base") for s in summaries
        ]
        comparison = compare_results(results)
        comparison["comparable"] = comparison["comparable"] and len(comparison["results"]) == len(
            results
        )
        comparison["selected_studies"] = [
            {
                "study_id": summary["study_id"],
                "status": row["status"],
                "error": row.get("error"),
                "metrics": row.get("metrics"),
            }
            for summary, row in zip(summaries, results)
        ]
        fields = set().union(*(s["recipe"].keys() for s in summaries))
        differences = {
            key: {s["study_id"]: s["recipe"].get(key) for s in summaries}
            for key in sorted(fields)
            if len({canonical(s["recipe"].get(key)) for s in summaries}) > 1
        }
        return {**comparison, "recipe_differences": differences}

    def note(self, study_id, body):
        identifier(study_id)
        if (
            not self.recipe_path(study_id).exists()
            or not isinstance(body, str)
            or not 1 <= len(body.strip()) <= 10000
        ):
            raise ValueError(
                "A saved study and nonempty note of at most 10000 characters are required"
            )
        with self.connect() as db:
            db.execute(
                "INSERT INTO notes(study_id,created_at,body) VALUES(?,?,?)",
                (study_id, utcnow(), body.strip()),
            )
        return {"saved": True}

    def _work(self):
        while not self.stopping.is_set():
            with self.connect() as db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1"
                ).fetchone()
                if row:
                    db.execute(
                        "UPDATE jobs SET status='running',updated_at=? WHERE id=?",
                        (utcnow(), row["id"]),
                    )
            if row is None:
                self.wakeup.wait(1)
                self.wakeup.clear()
                continue
            error, status = None, "failed"
            try:
                job = dict(row)
                recipe = json.loads(job["recipe"])
                if load_recipe(self.recipe_path(job["study_id"])) != recipe:
                    raise ValueError("Recipe changed after queue registration")
                request = self.managed("requests", job["id"] + ".yaml")
                if request.exists():
                    if load_recipe(request) != recipe:
                        raise ValueError("Frozen execution request changed")
                else:
                    with request.open("x", encoding="utf-8") as frozen:
                        yaml.safe_dump(recipe, frozen, allow_unicode=True, sort_keys=False)
                with self.managed("logs", job["id"] + ".log").open("a", encoding="utf-8") as log:
                    command = [
                        sys.executable,
                        "-m",
                        "quant_pipeline.research_workbench",
                        str(request),
                        "--output",
                        str(self.managed("runs", job["study_id"])),
                        "--expected-recipe-sha256",
                        digest(recipe),
                    ]
                    self.process = subprocess.Popen(
                        command,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
                    )
                    code = self.process.wait()
                    if code == 0:
                        from quant_report_hub.research_workbench import load_study

                        summary = load_study(self.managed("runs", job["study_id"], "study.json"))
                        if summary["recipe"] != recipe:
                            raise ValueError("Completed study differs from the queued recipe")
                    status = "completed" if code == 0 else "failed"
                    error = (
                        None
                        if code == 0
                        else f"Research exited with code {code}; inspect log and failed attempts"
                    )
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                error = str(exc)
            finally:
                self.process = None
                with self.connect() as db:
                    db.execute(
                        "UPDATE jobs SET status=?,error=?,updated_at=? WHERE id=?",
                        (status, error, utcnow(), row["id"]),
                    )

    def close(self):
        self.stopping.set()
        self.wakeup.set()
        if self.worker:
            if self.process is not None:
                self.process.terminate()
            self.worker.join(timeout=10)
            if self.worker.is_alive():
                raise RuntimeError("Worker is still stopping; console lock retained")
        if self.exclusive is not None:
            self.exclusive.__exit__(None, None, None)
            self.exclusive = None


def make_handler(console):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def send(self, status, body, content_type="application/json; charset=utf-8"):
            if not isinstance(body, bytes):
                body = canonical(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "SAMEORIGIN")
            self.end_headers()
            self.wfile.write(body)

        def host_valid(self):
            return self.headers.get("Host") == f"127.0.0.1:{self.server.server_port}"

        def do_GET(self):
            if not self.host_valid():
                return self.send(403, {"error": "Invalid Host"})
            path = unquote(urlsplit(self.path).path)
            try:
                if path == "/":
                    html = (
                        Path(__file__)
                        .with_name("research_console.html")
                        .read_text(encoding="utf-8")
                    )
                    return self.send(
                        200,
                        html.replace("__CONSOLE_TOKEN__", console.token).encode("utf-8"),
                        "text/html; charset=utf-8",
                    )
                if path == "/api/state":
                    return self.send(200, console.state())
                if path.startswith("/api/recipes/"):
                    return self.send(200, load_recipe(console.recipe_path(path.split("/")[-1])))
                if path.startswith("/accounts/"):
                    parts = path.split("/")
                    if len(parts) != 4 or parts[3] not in {
                        "account.html",
                        "account.json",
                        "evaluation.json",
                        "latest.json",
                    }:
                        raise ValueError("Invalid account artifact")
                    target = console.managed("accounts", identifier(parts[2]), parts[3])
                    kind = (
                        "text/html; charset=utf-8"
                        if parts[3].endswith(".html")
                        else "application/json; charset=utf-8"
                    )
                    return self.send(200, target.read_bytes(), kind)
                if path.startswith("/api/logs/"):
                    job = console.job(path.split("/")[-1])
                    log = console.managed("logs", job["id"] + ".log")
                    return self.send(
                        200,
                        {"log": log.read_text(encoding="utf-8")[-100000:] if log.exists() else ""},
                    )
                if path.startswith("/api/notes/"):
                    study_id = identifier(path.split("/")[-1])
                    with console.connect() as db:
                        notes = [
                            dict(row)
                            for row in db.execute(
                                "SELECT created_at,body FROM notes WHERE study_id=? ORDER BY id",
                                (study_id,),
                            )
                        ]
                    return self.send(200, notes)
                if path.startswith("/files/"):
                    parts = path.split("/", 3)
                    job = console.job(parts[2])
                    root = console.managed("runs", job["study_id"])
                    target = safe_child(root, parts[3])
                    if root not in target.parents or not target.is_file():
                        raise ValueError("Invalid artifact path")
                    kind = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
                    return self.send(
                        200,
                        target.read_bytes(),
                        kind + ("; charset=utf-8" if kind.startswith("text/") else ""),
                    )
                return self.send(404, {"error": "Not found"})
            except (ValueError, KeyError, IndexError, OSError) as exc:
                return self.send(400, {"error": str(exc)})

        def do_POST(self):
            origin = f"http://127.0.0.1:{self.server.server_port}"
            if (
                not self.host_valid()
                or self.headers.get("Origin", origin) != origin
                or not hmac.compare_digest(self.headers.get("X-Workbench-Token", ""), console.token)
            ):
                return self.send(403, {"error": "Invalid request origin or token"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if (
                    not 0 < size <= 262144
                    or self.headers.get("Content-Type", "").split(";")[0] != "application/json"
                ):
                    raise ValueError("Expected bounded JSON request")
                payload = json.loads(self.rfile.read(size))
                path = urlsplit(self.path).path
                if path == "/api/recipes":
                    result = console.save_recipe(payload["recipe"])
                elif path == "/api/clone":
                    result = console.clone(payload["study_id"], payload["new_id"])
                elif path == "/api/preflight":
                    result = console.preflight(payload["study_id"])
                elif path == "/api/run":
                    result = console.submit(payload["study_id"])
                elif path == "/api/compare":
                    result = console.compare(payload["job_ids"])
                elif path == "/api/notes":
                    result = console.note(payload["study_id"], payload["body"])
                elif path == "/api/advice":
                    result = console.advice(payload["query"], payload.get("study_id"))
                elif path == "/api/promote":
                    result = console.promote(
                        payload["job_id"],
                        payload["candidate"],
                        payload["account_id"],
                        payload["start"],
                        payload["end"],
                    )
                elif path == "/api/observe":
                    result = console.observe(
                        payload["account_id"], payload["study_id"], payload["as_of"]
                    )
                elif path == "/api/seal":
                    result = console.seal(payload["account_id"])
                else:
                    return self.send(404, {"error": "Not found"})
                return self.send(200, result)
            except (
                ValueError,
                TypeError,
                KeyError,
                OSError,
                RuntimeError,
                sqlite3.IntegrityError,
            ) as exc:
                return self.send(400, {"error": str(exc)})

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, action="append", required=True)
    parser.add_argument("--template", type=Path, action="append", default=[])
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    console = ResearchConsole(args.workspace, args.data_root)
    for path in args.template:
        recipe = load_recipe(path)
        if not console.recipe_path(recipe["study_id"]).exists():
            console.save_recipe(recipe)
    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(console))
    print(f"Research console: http://127.0.0.1:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        console.close()


if __name__ == "__main__":
    main()
