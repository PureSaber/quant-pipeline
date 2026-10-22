"""Local transactional research jobs, attempts, leases and fenced completion.

This store does not execute commands or claim exactly-once execution. It requires
a local SQLite filesystem; research artifacts remain in their owning store.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing, contextmanager
from pathlib import Path


class JobError(ValueError):
    """Invalid request or transition."""


class LeaseLost(JobError):
    """An attempt no longer owns a valid lease."""


class CancellationRequested(JobError):
    """A concurrent cancellation must be acknowledged before completion."""


def canonical(value: object) -> str:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    )


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise JobError(f"{name} must be a nonempty bounded string")
    return value


def _positive(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise JobError(f"{name} must be positive and finite")
    if not math.isfinite(value) or value <= 0:
        raise JobError(f"{name} must be positive and finite")
    return float(value)


def validate_refs(refs: object) -> list[dict]:
    if not isinstance(refs, list):
        raise JobError("artifacts must be a list")
    for ref in refs:
        if not isinstance(ref, dict) or set(ref) - {
            "schema_version",
            "sha256",
            "size_bytes",
            "uri",
            "media_type",
        }:
            raise JobError("invalid artifact reference")
        if ref.get("schema_version") != "quant.artifact-ref/v1":
            raise JobError("unsupported artifact reference schema")
        if not isinstance(ref.get("sha256"), str) or not re.fullmatch(
            r"[0-9a-f]{64}", ref["sha256"]
        ):
            raise JobError("invalid artifact hash")
        if type(ref.get("size_bytes")) is not int or ref["size_bytes"] < 0:
            raise JobError("invalid artifact size")
        _text(ref.get("uri"), "artifact uri")
        if "media_type" in ref:
            _text(ref["media_type"], "media_type")
    return refs


def validate_request(request: dict) -> str:
    required = {
        "schema_version",
        "kind",
        "project_id",
        "run_id",
        "payload",
        "inputs",
        "timeout_seconds",
        "max_attempts",
    }
    if not isinstance(request, dict) or set(request) != required:
        raise JobError("request fields do not match quant.job-request/v1")
    if request["schema_version"] != "quant.job-request/v1":
        raise JobError("unsupported job schema")
    for name in ("kind", "project_id", "run_id"):
        _text(request[name], name)
    if not isinstance(request["payload"], dict):
        raise JobError("payload must be an object")
    _positive(request["timeout_seconds"], "timeout_seconds")
    if type(request["max_attempts"]) is not int or not 1 <= request["max_attempts"] <= 100:
        raise JobError("max_attempts must be an integer in [1,100]")
    validate_refs(request["inputs"])
    return canonical(request)


class JobStore:
    """Each transition and its event are committed in one local transaction."""

    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time):
        self.path = Path(path).absolute()
        self.clock = clock
        if self.path.is_symlink():
            raise JobError("job database must not be a symlink")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise JobError(f"unsupported job database version {version}")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                  job_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                  request TEXT NOT NULL, request_hash TEXT NOT NULL, kind TEXT NOT NULL,
                  status TEXT NOT NULL, token INTEGER NOT NULL DEFAULT 0,
                  attempt_id TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
                  result TEXT, outputs TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at, job_id);
                CREATE TABLE IF NOT EXISTS attempts (
                  attempt_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id),
                  token INTEGER NOT NULL, worker_id TEXT NOT NULL, status TEXT NOT NULL,
                  started_at REAL NOT NULL, lease_expires_at REAL NOT NULL,
                  finished_at REAL, result TEXT, UNIQUE(job_id, token)
                );
                CREATE TABLE IF NOT EXISTS events (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
                  attempt_id TEXT, worker_id TEXT, timestamp REAL NOT NULL,
                  event TEXT NOT NULL, details TEXT NOT NULL,
                  FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE INDEX IF NOT EXISTS events_job ON events(job_id, sequence);
                PRAGMA user_version=1;
            """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        db = self._connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _now(self) -> float:
        now = self.clock()
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise JobError("clock must return finite Unix seconds")
        return float(now)

    @staticmethod
    def _event(db, job_id, now, event, *, attempt_id=None, worker_id=None, **details):
        db.execute(
            "INSERT INTO events(job_id,attempt_id,worker_id,timestamp,event,details) "
            "VALUES(?,?,?,?,?,?)",
            (job_id, attempt_id, worker_id, now, event, canonical(details)),
        )

    @staticmethod
    def _job(row) -> dict:
        if row is None:
            raise JobError("job does not exist")
        value = dict(row)
        for name in ("request", "result", "outputs"):
            value[name] = json.loads(value[name]) if value[name] is not None else None
        value["cancel_requested"] = bool(value["cancel_requested"])
        return value

    def get(self, job_id: str) -> dict:
        db = self._connect()
        try:
            return self._job(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())
        finally:
            db.close()

    def list(
        self, *, limit: int = 100, status: str | None = None, before: str | None = None
    ) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise JobError("limit must be in [1,1000]")
        db = self._connect()
        try:
            query = "SELECT * FROM jobs"
            args: list = []
            conditions = []
            if status is not None:
                conditions.append("status=?")
                args.append(status)
            if before is not None:
                cursor = db.execute(
                    "SELECT created_at,job_id FROM jobs WHERE job_id=?", (before,)
                ).fetchone()
                if cursor is None:
                    raise JobError("history cursor does not exist")
                conditions.append("(created_at,job_id) < (?,?)")
                args.extend(cursor)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY created_at DESC, job_id DESC LIMIT ?"
            return [self._job(row) for row in db.execute(query, (*args, limit))]
        finally:
            db.close()

    def submit(self, request: dict, idempotency_key: str) -> dict:
        with self._transaction() as db:
            return self._submit(db, request, idempotency_key)

    def _submit(self, db, request: dict, idempotency_key: str) -> dict:
        """Submit within the caller's transaction (also used for atomic batches)."""
        encoded = validate_request(request)
        _text(idempotency_key, "idempotency_key")
        identity = hashlib.sha256(encoded.encode()).hexdigest()
        previous = db.execute(
            "SELECT * FROM jobs WHERE idempotency_key=?", (idempotency_key,)
        ).fetchone()
        if previous is not None:
            if previous["request"] != encoded:
                raise JobError("idempotency key conflicts with request identity")
            return self._job(previous)
        job_id, now = uuid.uuid4().hex, self._now()
        db.execute(
            "INSERT INTO jobs(job_id,idempotency_key,request,request_hash,kind,status,"
            "created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?)",
            (job_id, idempotency_key, encoded, identity, request["kind"], now, now),
        )
        self._event(
            db,
            job_id,
            now,
            "submitted",
            request_hash=identity,
            project_id=request["project_id"],
            run_id=request["run_id"],
        )
        return self._job(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())

    def _expire(self, db, now):
        rows = db.execute(
            "SELECT a.*,j.request,j.cancel_requested FROM attempts a JOIN jobs j "
            "ON j.attempt_id=a.attempt_id WHERE a.status='running' "
            "AND a.lease_expires_at<=?",
            (now,),
        ).fetchall()
        for row in rows:
            request = json.loads(row["request"])
            status = (
                "cancelled"
                if row["cancel_requested"]
                else "queued"
                if row["token"] < request["max_attempts"]
                else "failed"
            )
            db.execute(
                "UPDATE attempts SET status='expired',finished_at=? WHERE attempt_id=?",
                (now, row["attempt_id"]),
            )
            db.execute(
                "UPDATE jobs SET status=?,updated_at=? WHERE job_id=?", (status, now, row["job_id"])
            )
            self._event(
                db,
                row["job_id"],
                now,
                "lease_expired",
                attempt_id=row["attempt_id"],
                worker_id=row["worker_id"],
                next_status=status,
                process_termination_confirmed=False,
            )

    def claim(
        self, worker_id: str, lease_seconds: float = 30, *, kinds: Sequence[str] | None = None
    ) -> dict | None:
        _text(worker_id, "worker_id")
        lease_seconds = _positive(lease_seconds, "lease_seconds")
        if kinds is not None:
            if isinstance(kinds, str) or not kinds:
                raise JobError("kinds must be a nonempty sequence")
            for kind in kinds:
                _text(kind, "kind")
        with self._transaction() as db:
            now = self._now()
            self._expire(db, now)
            query, args = "SELECT * FROM jobs WHERE status='queued'", []
            if kinds is not None:
                query += " AND kind IN (" + ",".join("?" for _ in kinds) + ")"
                args.extend(kinds)
            row = db.execute(query + " ORDER BY created_at,job_id LIMIT 1", args).fetchone()
            if row is None:
                return None
            attempt_id, token = uuid.uuid4().hex, row["token"] + 1
            db.execute(
                "INSERT INTO attempts VALUES(?,?,?,?, 'running',?,?,NULL,NULL)",
                (attempt_id, row["job_id"], token, worker_id, now, now + lease_seconds),
            )
            db.execute(
                "UPDATE jobs SET status='running',token=?,attempt_id=?,updated_at=? WHERE job_id=?",
                (token, attempt_id, now, row["job_id"]),
            )
            self._event(
                db,
                row["job_id"],
                now,
                "claimed",
                attempt_id=attempt_id,
                worker_id=worker_id,
                fencing_token=token,
            )
            return {
                "job": self._job(
                    db.execute("SELECT * FROM jobs WHERE job_id=?", (row["job_id"],)).fetchone()
                ),
                "attempt_id": attempt_id,
                "fencing_token": token,
                "worker_id": worker_id,
                "lease_expires_at": now + lease_seconds,
            }

    def _owned(self, db, attempt_id, token, now):
        if type(token) is not int:
            raise LeaseLost("invalid fencing token")
        row = db.execute(
            "SELECT a.*,j.cancel_requested,j.request FROM attempts a JOIN jobs j "
            "ON j.attempt_id=a.attempt_id WHERE a.attempt_id=? AND a.token=? "
            "AND a.status='running' AND j.status='running' AND j.token=a.token "
            "AND a.lease_expires_at>?",
            (attempt_id, token, now),
        ).fetchone()
        if row is None:
            raise LeaseLost("attempt lease expired or was superseded")
        return row

    def heartbeat(self, attempt_id: str, token: int, lease_seconds: float = 30) -> dict:
        lease_seconds = _positive(lease_seconds, "lease_seconds")
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, attempt_id, token, now)
            db.execute(
                "UPDATE attempts SET lease_expires_at=? WHERE attempt_id=?",
                (now + lease_seconds, attempt_id),
            )
            return {
                "cancel_requested": bool(row["cancel_requested"]),
                "lease_expires_at": now + lease_seconds,
            }

    def cancel(self, job_id: str) -> dict:
        with self._transaction() as db:
            return self._cancel(db, job_id)

    def _cancel(self, db, job_id: str) -> dict:
        """Cancel within the caller's transaction."""
        row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        job = self._job(row)
        if job["status"] not in ("queued", "running") or job["cancel_requested"]:
            return job
        now = self._now()
        status = "cancelled" if job["status"] == "queued" else "running"
        db.execute(
            "UPDATE jobs SET cancel_requested=1,status=?,updated_at=? WHERE job_id=?",
            (status, now, job_id),
        )
        self._event(db, job_id, now, "cancel_requested", attempt_id=job["attempt_id"])
        return self._job(db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone())

    def record_event(self, attempt_id: str, token: int, event: str, **details) -> None:
        """Append execution evidence only while the caller still owns its attempt."""
        _text(event, "event")
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, attempt_id, token, now)
            self._event(
                db,
                row["job_id"],
                now,
                event,
                attempt_id=attempt_id,
                worker_id=row["worker_id"],
                **details,
            )

    def finish(
        self,
        attempt_id: str,
        token: int,
        result: dict,
        outputs: list[dict] | None = None,
        *,
        status: str = "succeeded",
        retryable: bool = False,
    ) -> dict:
        if status not in ("succeeded", "failed", "cancelled") or not isinstance(result, dict):
            raise JobError("invalid completion")
        if type(retryable) is not bool or (retryable and status != "failed"):
            raise JobError("only failures may be retryable")
        outputs = validate_refs([] if outputs is None else outputs)
        if status != "succeeded" and outputs:
            raise JobError("failed attempts cannot publish successful outputs")
        encoded, encoded_outputs = canonical(result), canonical(outputs)
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, attempt_id, token, now)
            if row["cancel_requested"] and status != "cancelled":
                raise CancellationRequested("cancellation must be acknowledged before completion")
            target = status
            if retryable and token < json.loads(row["request"])["max_attempts"]:
                target = "queued"
            db.execute(
                "UPDATE attempts SET status=?,finished_at=?,result=? WHERE attempt_id=?",
                (status, now, encoded, attempt_id),
            )
            db.execute(
                "UPDATE jobs SET status=?,result=?,outputs=?,updated_at=? WHERE job_id=?",
                (target, encoded, encoded_outputs, now, row["job_id"]),
            )
            self._event(
                db,
                row["job_id"],
                now,
                "completed",
                attempt_id=attempt_id,
                worker_id=row["worker_id"],
                status=status,
                next_status=target,
                fencing_token=token,
                outputs=outputs,
            )
            return self._job(
                db.execute("SELECT * FROM jobs WHERE job_id=?", (row["job_id"],)).fetchone()
            )

    def attempts(self, job_id: str) -> list[dict]:
        self.get(job_id)
        db = self._connect()
        try:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM attempts WHERE job_id=? ORDER BY token", (job_id,)
                )
            ]
            for row in rows:
                row["result"] = json.loads(row["result"]) if row["result"] else None
            return rows
        finally:
            db.close()

    def events(self, job_id: str) -> list[dict]:
        self.get(job_id)
        db = self._connect()
        try:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM events WHERE job_id=? ORDER BY sequence", (job_id,)
                )
            ]
            for row in rows:
                row["details"] = json.loads(row["details"])
            return rows
        finally:
            db.close()
