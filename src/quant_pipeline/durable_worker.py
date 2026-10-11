"""Independent local worker for trusted, explicitly registered research handlers.

Handlers run in attempt-specific subprocesses. This is lifecycle separation, not
an OS sandbox. The task request cannot select Python imports, commands or paths.
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path

from .durable_jobs import (
    CancellationRequested,
    JobError,
    JobStore,
    LeaseLost,
    _positive,
    canonical,
    validate_refs,
)


def resolve_handler(spec: str) -> Callable:
    """Resolve an administrator-supplied import, never a request payload value."""
    module, separator, name = spec.partition(":")
    if not separator or not module or not name.isidentifier():
        raise JobError("handler must be module:function")
    handler = getattr(importlib.import_module(module), name)
    if not callable(handler):
        raise JobError("handler must be callable")
    return handler


def terminate_tree(process: subprocess.Popen) -> None:
    """Terminate a running child and its process group before acknowledging it."""
    if process.poll() is not None:
        return
    if os.name == "nt":
        completed = subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            capture_output=True,
            check=False,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if completed.returncode and process.poll() is None:
            raise RuntimeError("could not terminate worker process tree")
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait(timeout=15)


class Worker:
    """A local worker; restart recovery occurs when another worker claims jobs.

    artifact_guard(owner, refs) must atomically retain and verify immutable objects
    including snapshot members. It must resolve refs in an administrator-configured
    store, never trust ref.uri as an arbitrary local path. Retentions outlive jobs;
    this worker performs no automatic garbage collection.
    """

    def __init__(
        self,
        store: JobStore,
        workspace: str | Path,
        handlers: Mapping[str, str],
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30,
        artifact_guard: Callable[[str, list[dict]], None] | None = None,
    ):
        if not handlers:
            raise JobError("at least one registered handler is required")
        for spec in handlers.values():
            resolve_handler(spec)
        self.store, self.handlers = store, dict(handlers)
        self.workspace = Path(workspace).absolute()
        if self.workspace.is_symlink():
            raise JobError("worker workspace must not be a symlink")
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.worker_id = worker_id or uuid.uuid4().hex
        self.lease_seconds = _positive(lease_seconds, "lease_seconds")
        self.artifact_guard = artifact_guard

    def _retain(self, owner: str, refs: list[dict]) -> None:
        validate_refs(refs)
        if refs:
            if self.artifact_guard is None:
                raise JobError("artifact references require a configured verifying retention guard")
            self.artifact_guard(owner, refs)

    def run_once(self) -> dict | None:
        claim = self.store.claim(self.worker_id, self.lease_seconds, kinds=list(self.handlers))
        if claim is None:
            return None
        job, attempt, token = claim["job"], claim["attempt_id"], claim["fencing_token"]
        stop, cancelled, lost = threading.Event(), threading.Event(), threading.Event()

        def heartbeat():
            while not stop.wait(self.lease_seconds / 4):
                try:
                    state = self.store.heartbeat(attempt, token, self.lease_seconds)
                    if state["cancel_requested"]:
                        cancelled.set()
                except (sqlite3.Error, JobError, OSError):
                    # Ownership cannot be established after any database failure.
                    lost.set()
                    return

        thread = threading.Thread(target=heartbeat, name=f"lease-{attempt}", daemon=True)
        thread.start()
        process = None
        outcome, status, outputs = {}, "failed", []
        fatal = None
        directory = self.workspace / attempt
        try:
            directory.mkdir(exist_ok=False)
            request_path, result_path = directory / "request.json", directory / "result.json"
            request_path.write_text(canonical(job["request"]), encoding="utf-8")
            (directory / "context.json").write_text(
                canonical(
                    {
                        "schema_version": "quant.attempt-context/v1",
                        "job_id": job["job_id"],
                        "attempt_id": attempt,
                        "fencing_token": token,
                        "worker_id": self.worker_id,
                        "project_id": job["request"]["project_id"],
                        "run_id": job["request"]["run_id"],
                    }
                ),
                encoding="utf-8",
            )
            self._retain(f"job/{job['job_id']}/inputs", job["request"]["inputs"])
            state = self.store.heartbeat(attempt, token, self.lease_seconds)
            if state["cancel_requested"]:
                cancelled.set()
            if not cancelled.is_set() and not lost.is_set():
                command = [
                    sys.executable,
                    "-m",
                    "quant_pipeline.durable_worker",
                    "--child",
                    "--handler",
                    self.handlers[job["request"]["kind"]],
                    "--request",
                    str(request_path),
                    "--result",
                    str(result_path),
                    "--workspace",
                    str(directory),
                ]
                options = (
                    {"creationflags": subprocess.CREATE_NO_WINDOW}
                    if os.name == "nt"
                    else {"start_new_session": True}
                )
                with (
                    (directory / "stdout.log").open("wb") as stdout,
                    (directory / "stderr.log").open("wb") as stderr,
                ):
                    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, **options)
                    self.store.record_event(
                        attempt, token, "process_started", pid=process.pid, workspace=str(directory)
                    )
                    started = time.monotonic()
                    while process.poll() is None:
                        if cancelled.is_set() or lost.is_set():
                            terminate_tree(process)
                            break
                        if time.monotonic() - started >= job["request"]["timeout_seconds"]:
                            terminate_tree(process)
                            raise TimeoutError("research process exceeded timeout_seconds")
                        time.sleep(min(0.05, self.lease_seconds / 8))
                if not cancelled.is_set() and not lost.is_set():
                    if process.returncode:
                        raise JobError(f"research handler exited with code {process.returncode}")
                    if result_path.is_symlink() or result_path.stat().st_size > 1024 * 1024:
                        raise JobError(
                            "handler result must be a regular JSON file of at most 1 MiB"
                        )
                    result = json.loads(result_path.read_text(encoding="utf-8"))
                    if (
                        not isinstance(result, dict)
                        or set(result) != {"result", "outputs"}
                        or not isinstance(result["result"], dict)
                    ):
                        raise JobError("handler must return result object and outputs array")
                    canonical(result)
                    outputs = validate_refs(result["outputs"])
                    self._retain(f"job/{job['job_id']}/attempt/{attempt}/outputs", outputs)
                    outcome, status = result["result"], "succeeded"
        except BaseException as exc:  # noqa: BLE001 - contain trusted plugin errors; rethrow interrupts
            if process is not None:
                try:
                    terminate_tree(process)
                except (OSError, subprocess.SubprocessError, RuntimeError):
                    # Never acknowledge a failure/cancellation if termination was not confirmed.
                    lost.set()
            outcome, status, outputs = (
                {"error_type": type(exc).__name__, "error": str(exc)},
                "failed",
                [],
            )
            if not isinstance(exc, Exception):
                fatal = exc
            if isinstance(exc, LeaseLost):
                lost.set()
        finally:
            stop.set()
            thread.join(timeout=35)
        if thread.is_alive() or lost.is_set():
            if fatal:
                raise fatal
            return {"job_id": job["job_id"], "attempt_id": attempt, "status": "lease_lost"}
        if cancelled.is_set():
            outcome, status, outputs = {"cancelled": True}, "cancelled", []
        try:
            try:
                finished = self.store.finish(
                    attempt, token, outcome, outputs, status=status, retryable=status == "failed"
                )
            except CancellationRequested:
                # The process is already stopped, so this also handles cancel/finish races.
                finished = self.store.finish(
                    attempt, token, {"cancelled": True}, status="cancelled"
                )
        except LeaseLost:
            finished = {"job_id": job["job_id"], "attempt_id": attempt, "status": "lease_lost"}
        if fatal:
            raise fatal
        return finished


def _child(args) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    request = json.loads(Path(args.request).read_text(encoding="utf-8"))
    result = resolve_handler(args.handler[0])(request, Path(args.workspace))
    destination = Path(args.result)
    encoded = canonical(result)
    if len(encoded.encode("utf-8")) > 1024 * 1024:
        raise JobError("handler result exceeds 1 MiB")
    temporary = destination.with_suffix(".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(destination)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--handler", action="append", required=True)
    parser.add_argument("--artifact-guard", help="administrator-configured module:function")
    parser.add_argument("--worker-id")
    parser.add_argument("--lease-seconds", type=float, default=30)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--request", help=argparse.SUPPRESS)
    parser.add_argument("--result", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.child:
        return _child(args)
    if not args.database:
        parser.error("--database is required")
    handlers = {}
    for value in args.handler:
        kind, separator, spec = value.partition("=")
        if not separator or not kind or kind in handlers:
            parser.error("each --handler must be a distinct kind=module:function")
        handlers[kind] = spec
    worker = Worker(
        JobStore(args.database),
        args.workspace,
        handlers,
        worker_id=args.worker_id,
        lease_seconds=args.lease_seconds,
        artifact_guard=resolve_handler(args.artifact_guard) if args.artifact_guard else None,
    )
    try:
        while True:
            result = worker.run_once()
            if result is not None:
                print(
                    canonical({key: result.get(key) for key in ("job_id", "attempt_id", "status")}),
                    flush=True,
                )
            if args.once:
                return 0
            if result is None:
                time.sleep(0.25)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
