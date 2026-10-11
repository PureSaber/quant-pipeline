"""Trusted local adapter joining durable jobs to the optional QDK/Lab stack."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .durable_jobs import JobStore, canonical, validate_request


def submit_research_batch(store: JobStore, request: dict, idempotency_key: str) -> dict:
    """Verify/pin the union once, then publish every child in one job transaction."""
    from .durable_batches import BatchStore, shared_refs, validate_batch

    request = json.loads(canonical(request))
    children = validate_batch(request)
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        raise ValueError("nonempty idempotency key required")
    owner = "batch-submission/" + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    retain_artifacts(owner, shared_refs(*(child["inputs"] for _, child in children)))
    return BatchStore(store).submit(request, idempotency_key)


def retry_research_batch(store: JobStore, batch_id: str, key: str, candidates: list[str]) -> dict:
    """Keep original pins and reverify before making retry jobs visible."""
    from .durable_batches import BatchStore, shared_refs

    batches = BatchStore(store)
    original = batches.get(batch_id)
    refs = shared_refs(
        *(
            member["job"]["request"]["inputs"]
            for member in original["members"]
            if member["candidate_id"] in candidates
        )
    )
    owner = "batch-submission/" + hashlib.sha256(key.encode("utf-8")).hexdigest()
    retain_artifacts(owner, refs)
    return batches.retry(batch_id, key, candidates)


def retain_artifacts(owner: str, refs: list[dict]) -> None:
    """Resolve and retain by content identity in a locally configured QDK store."""
    from quant_data_kit.artifacts import ArtifactRef, ArtifactStore

    store = ArtifactStore(Path(os.environ["QUANT_RESEARCH_STORE_ROOT"]), context={"owner": owner})
    store.retain(owner, [ArtifactRef.from_dict(ref) for ref in refs])


def submit_research(store: JobStore, request: dict, idempotency_key: str) -> dict:
    """Protect declared inputs before making the task visible to any worker.

    Conflicting submissions may leave conservative pins; neither this adapter nor
    the worker automatically releases them. The caller owns retention policy.
    """
    validate_request(request)
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        raise ValueError("nonempty idempotency key required")
    owner = "submission/" + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    retain_artifacts(owner, request["inputs"])
    return store.submit(request, idempotency_key)
