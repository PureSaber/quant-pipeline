"""Trusted local adapter joining durable jobs to the optional QDK/Lab stack."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .durable_jobs import JobStore, validate_request


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
