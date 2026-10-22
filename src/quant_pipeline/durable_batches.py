"""Atomic collections of ordinary durable jobs, with immutable retry lineage.

There is no second executor: every candidate uses JobStore and Worker unchanged.
The batch status is a consistent snapshot of child states, not another lease.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from contextlib import closing, contextmanager
from copy import deepcopy

from .durable_jobs import JobError, JobStore, _text, canonical, validate_refs, validate_request

_SCHEMA = "quant.job-batch/v1"
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z")
_TERMINAL = {"succeeded", "failed", "cancelled"}


def shared_refs(*groups: list[dict]) -> list[dict]:
    """Deduplicate complete references; reject contradictory content identities."""
    found = {}
    for group in groups:
        for ref in validate_refs(group):
            key = ref["sha256"]
            if key in found and found[key] != ref:
                raise JobError("contradictory references for the same input hash")
            found[key] = ref
    return list(found.values())


def validate_batch(request: dict) -> list[tuple[str, dict]]:
    if not isinstance(request, dict) or set(request) != {
        "schema_version",
        "project_id",
        "run_id",
        "shared_inputs",
        "candidates",
    }:
        raise JobError("request fields do not match quant.job-batch/v1")
    if request["schema_version"] != _SCHEMA:
        raise JobError("unsupported batch schema")
    for field in ("project_id", "run_id"):
        _text(request[field], field)
    shared_refs(request["shared_inputs"])
    candidates = request["candidates"]
    if not isinstance(candidates, list) or not 1 <= len(candidates) <= 1000:
        raise JobError("batch requires 1 to 1000 candidates")
    children, seen = [], set()
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) != {
            "candidate_id",
            "kind",
            "payload",
            "inputs",
            "timeout_seconds",
            "max_attempts",
        }:
            raise JobError("invalid candidate fields")
        identity = candidate["candidate_id"]
        if not isinstance(identity, str) or not _ID.fullmatch(identity) or identity in seen:
            raise JobError("candidate_id must be a unique simple identifier")
        seen.add(identity)
        child = {
            "schema_version": "quant.job-request/v1",
            "project_id": request["project_id"],
            "run_id": request["run_id"] + "/" + identity,
            **{key: value for key, value in candidate.items() if key != "candidate_id"},
        }
        child["inputs"] = shared_refs(request["shared_inputs"], candidate["inputs"])
        validate_request(child)
        children.append((identity, child))
    if len(canonical(request).encode("utf-8")) > 4 * 1024 * 1024:
        raise JobError("batch request exceeds 4 MiB")
    return children


class BatchStore:
    """Batch membership lives in the same transaction/database as its jobs.

    Tables are additive to the v1 job database. Old clients/workers still read and
    execute child jobs. Concurrent cancellation/claim obey existing JobStore rules.
    """

    def __init__(self, jobs: JobStore):
        self.jobs = jobs
        with jobs._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS research_batches (
                batch_id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
                request TEXT NOT NULL, retry_of TEXT REFERENCES research_batches(batch_id),
                created_at REAL NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS research_batch_members (
                batch_id TEXT NOT NULL REFERENCES research_batches(batch_id),
                candidate_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
                source_job_id TEXT REFERENCES jobs(job_id),
                PRIMARY KEY(batch_id, candidate_id), UNIQUE(batch_id, ordinal))""")
            db.execute(
                "CREATE INDEX IF NOT EXISTS research_batches_recent "
                "ON research_batches(created_at DESC,batch_id DESC)"
            )

    @contextmanager
    def _read(self):
        # WAL readers see one snapshot without holding a writer reservation and
        # delaying live worker heartbeats while rendering a large batch.
        with closing(self.jobs._connect()) as db:
            db.execute("BEGIN")
            yield db

    def _get(self, db, batch_id: str) -> dict:
        row = db.execute("SELECT * FROM research_batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise JobError("batch does not exist")
        batch = dict(row)
        batch["schema_version"] = "quant.job-batch-state/v1"
        batch["request"] = json.loads(batch["request"])
        batch["members"] = [
            {
                "candidate_id": member["candidate_id"],
                "source_job_id": member["source_job_id"],
                "job": self.jobs._job(
                    db.execute("SELECT * FROM jobs WHERE job_id=?", (member["job_id"],)).fetchone()
                ),
            }
            for member in db.execute(
                "SELECT * FROM research_batch_members WHERE batch_id=? ORDER BY ordinal",
                (batch_id,),
            ).fetchall()
        ]
        counts = Counter(member["job"]["status"] for member in batch["members"])
        batch["counts"] = {
            state: counts[state]
            for state in ("queued", "running", "succeeded", "failed", "cancelled")
        }
        if counts["running"]:
            status = "running"
        elif counts["queued"]:
            status = "queued"
        elif counts["failed"]:
            status = "failed"
        elif counts["cancelled"]:
            status = "cancelled"
        else:
            status = "succeeded"
        batch["status"] = status
        batch["terminal"] = all(member["job"]["status"] in _TERMINAL for member in batch["members"])
        return batch

    def get(self, batch_id: str) -> dict:
        with self._read() as db:
            return self._get(db, batch_id)

    def list(
        self, *, limit: int = 100, project_id: str | None = None, before: str | None = None
    ) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise JobError("limit must be in [1,1000]")
        with self._read() as db:
            query = "SELECT batch_id,request FROM research_batches"
            args = ()
            if before is not None:
                cursor = db.execute(
                    "SELECT created_at,batch_id FROM research_batches WHERE batch_id=?", (before,)
                ).fetchone()
                if cursor is None:
                    raise JobError("history cursor does not exist")
                query += " WHERE (created_at,batch_id) < (?,?)"
                args = tuple(cursor)
            rows = db.execute(query + " ORDER BY created_at DESC,batch_id DESC", args)
            selected = []
            for row in rows:
                if project_id is None or json.loads(row["request"])["project_id"] == project_id:
                    selected.append(row["batch_id"])
                    if len(selected) == limit:
                        break
            return [self._get(db, identity) for identity in selected]

    def _submit(self, db, request, key, *, retry_of=None, sources=None):
        children = validate_batch(request)
        _text(key, "idempotency_key")
        encoded = canonical(request)
        row = db.execute(
            "SELECT * FROM research_batches WHERE idempotency_key=?", (key,)
        ).fetchone()
        if row is not None:
            if row["request"] != encoded or row["retry_of"] != retry_of:
                raise JobError("batch idempotency key conflicts with request identity")
            return self._get(db, row["batch_id"])
        batch_id = uuid.uuid4().hex
        db.execute(
            "INSERT INTO research_batches VALUES(?,?,?,?,?)",
            (batch_id, key, encoded, retry_of, self.jobs._now()),
        )
        for ordinal, (candidate_id, child) in enumerate(children):
            child_key = (
                "batch/"
                + hashlib.sha256(canonical([key, candidate_id]).encode("utf-8")).hexdigest()
            )
            job = self.jobs._submit(db, child, child_key)
            db.execute(
                "INSERT INTO research_batch_members VALUES(?,?,?,?,?)",
                (batch_id, candidate_id, ordinal, job["job_id"], (sources or {}).get(candidate_id)),
            )
            self.jobs._event(
                db,
                job["job_id"],
                self.jobs._now(),
                "batch_member",
                batch_id=batch_id,
                candidate_id=candidate_id,
                source_job_id=(sources or {}).get(candidate_id),
            )
        return self._get(db, batch_id)

    def submit(self, request: dict, idempotency_key: str) -> dict:
        # Decouple caller-owned nested objects before entering the transaction.
        request = json.loads(canonical(request))
        with self.jobs._transaction() as db:
            return self._submit(db, request, idempotency_key)

    def cancel(self, batch_id: str) -> dict:
        with self.jobs._transaction() as db:
            batch = self._get(db, batch_id)
            for member in batch["members"]:
                self.jobs._cancel(db, member["job"]["job_id"])
            return self._get(db, batch_id)

    def retry(self, batch_id: str, idempotency_key: str, candidate_ids: list[str]) -> dict:
        """Explicit failed/cancelled selection creates new jobs; never resets history."""
        if (
            not isinstance(candidate_ids, list)
            or not candidate_ids
            or any(not isinstance(x, str) for x in candidate_ids)
            or len(candidate_ids) != len(set(candidate_ids))
        ):
            raise JobError("retry requires unique candidate_ids")
        with self.jobs._transaction() as db:
            original = self._get(db, batch_id)
            members = {item["candidate_id"]: item["job"] for item in original["members"]}
            if any(
                name not in members or members[name]["status"] not in {"failed", "cancelled"}
                for name in candidate_ids
            ):
                raise JobError("only failed or cancelled candidates may be explicitly retried")
            request = deepcopy(original["request"])
            # Preserve original order, independent of selection order.
            request["candidates"] = [
                item for item in request["candidates"] if item["candidate_id"] in candidate_ids
            ]
            return self._submit(
                db,
                request,
                idempotency_key,
                retry_of=batch_id,
                sources={name: members[name]["job_id"] for name in candidate_ids},
            )
