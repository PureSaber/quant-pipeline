"""Optional Linux Docker execution boundary; never falls back to a host process.

The Docker daemon, image and host administrator are trusted. Only /inputs is
bind-mounted, read-only. Mutable files live in bounded container tmpfs mounts.
Outputs are streamed before removing the container (stopping loses tmpfs).
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import platform
import re
import shutil
import signal
import stat
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath


class IsolationError(RuntimeError):
    """Requested containment cannot be supplied; execution is refused."""


DEFAULTS = {
    "memory_mb": 512,
    "cpus": 1,
    "pids": 64,
    "work_mb": 32,
    "tmp_mb": 32,
    "input_bytes": 256 * 1024**2,
    "output_bytes": 16 * 1024**2,
    "log_bytes": 1024**2,
    "timeout_seconds": 60,
    "seed": 42,
}
_RANGES = {
    "memory_mb": (64, 8192),
    "cpus": (1, 8),
    "pids": (16, 512),
    "work_mb": (1, 1024),
    "tmp_mb": (1, 256),
    "input_bytes": (1, 1024**3),
    "output_bytes": (1, 256 * 1024**2),
    "log_bytes": (1, 16 * 1024**2),
    "timeout_seconds": (1, 86400),
    "seed": (0, 2**32 - 1),
}
_MAX_FILES = 4096


def validate_policy(value):
    if not isinstance(value, dict) or set(value) - (set(DEFAULTS) | {"schema", "backend", "image"}):
        raise IsolationError("Unknown isolation policy fields")
    if value.get("schema") != "quant.research-isolation/v1":
        raise IsolationError("Expected quant.research-isolation/v1")
    if value.get("backend") != "docker-linux":
        raise IsolationError("Only docker-linux is an enforced backend")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", str(value.get("image", ""))):
        raise IsolationError("image must be a preinstalled immutable Docker sha256 image ID")
    result = {**DEFAULTS, **value}
    for key, (low, high) in _RANGES.items():
        if type(result[key]) is not int or not low <= result[key] <= high:
            raise IsolationError(f"{key} must be an integer in [{low}, {high}]")
    return result


def _sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False), "utf-8")


class Docker:
    def __init__(self):
        if platform.system() != "Linux":
            raise IsolationError("docker-linux isolation requires a Linux host; no local fallback")
        executable = shutil.which("docker")
        if not executable:
            raise IsolationError(
                "Docker CLI unavailable; install/configure it explicitly as operator"
            )
        # Ignore DOCKER_HOST/CONTEXT/TLS configuration and client proxy settings.
        # Only this local daemon is supported; remote daemons cannot see staging.
        self.config = tempfile.TemporaryDirectory(prefix="quant-docker-config-")
        self.base = [
            executable,
            "--config",
            self.config.name,
            "--host",
            "unix:///var/run/docker.sock",
        ]
        self.env = {key: os.environ[key] for key in ("PATH",) if key in os.environ}
        self.env.update(HOME="/nonexistent", DOCKER_CONFIG="/nonexistent/quant-isolation")

    def close(self):
        self.config.cleanup()

    def call(self, *args, check=True):
        try:
            result = subprocess.run(
                [*self.base, *args], env=self.env, capture_output=True, timeout=30, check=False
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise IsolationError(f"Docker control operation failed: {exc}") from exc
        if check and result.returncode:
            raise IsolationError(result.stderr.decode("utf-8", "replace")[-4000:])
        return result

    def inspect(self, *args):
        return json.loads(self.call(*args).stdout)


def preflight(policy):
    policy = validate_policy(policy)
    docker = Docker()
    try:
        return _preflight(docker, policy)
    finally:
        docker.close()


def _preflight(docker, policy):
    info = docker.inspect("info", "--format", "{{json .}}")
    if info.get("OSType") != "linux" or str(info.get("CgroupVersion")) != "2":
        raise IsolationError("Requires Linux Docker and cgroup v2")
    required = ("MemoryLimit", "SwapLimit", "PidsLimit", "CpuCfsQuota")
    # Docker's field uses CpuCfsQuota (not CPUCfsQuota).
    if any(info.get(key) is not True for key in required):
        raise IsolationError("Daemon cannot enforce memory/swap/PID/CPU limits")
    if not any(
        "name=seccomp" in item and "profile=builtin" in item
        for item in info.get("SecurityOptions", [])
    ):
        raise IsolationError("Requires the builtin Docker seccomp profile")
    image = docker.inspect("image", "inspect", policy["image"])[0]
    if image.get("Id") != policy["image"] or image.get("Os") != "linux":
        raise IsolationError("Preinstalled Linux image identity mismatch")
    # Image-defined volumes create extra writable storage outside our tmpfs quota.
    if image.get("Config", {}).get("Volumes"):
        raise IsolationError("Images with declared VOLUME mounts are unsupported")
    return {
        "backend": "docker-linux",
        "image_id": image["Id"],
        "image_architecture": image.get("Architecture"),
        "docker_version": info.get("ServerVersion"),
        "kernel": info.get("KernelVersion"),
        "cgroup_version": str(info["CgroupVersion"]),
        "security_options": info["SecurityOptions"],
    }


def _stage(source, target, limit):
    source = Path(source).absolute()
    if source.resolve() != source or not source.is_dir():
        raise IsolationError("inputs must be a real directory, without symlink parents")
    hashes, total, entries = {}, 0, 0
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK

    def walk(parent_fd, relative_root):
        nonlocal total, entries
        for name in sorted(os.listdir(parent_fd)):
            entries += 1
            if entries > _MAX_FILES or len(relative_root.parts) > 64:
                raise IsolationError("Input tree entry/depth limit exceeded")
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            relative = relative_root / name
            if not stat.S_ISREG(before.st_mode) and not stat.S_ISDIR(before.st_mode):
                raise IsolationError("Input links, FIFOs and special files are forbidden")
            fd = os.open(name, flags, dir_fd=parent_fd)
            opened = os.fstat(fd)
            if (before.st_dev, before.st_ino, before.st_mode) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
            ):
                os.close(fd)
                raise IsolationError("Input changed while opening")
            if stat.S_ISDIR(opened.st_mode):
                try:
                    walk(fd, relative)
                finally:
                    os.close(fd)
                continue
            if len(hashes) >= _MAX_FILES:
                os.close(fd)
                raise IsolationError("Too many input files")
            with os.fdopen(fd, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise IsolationError("Only regular input files are supported")
                destination = target / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with destination.open("xb") as output:
                    for chunk in iter(lambda: stream.read(1024**2), b""):
                        total += len(chunk)
                        if total > limit:
                            raise IsolationError("Input byte limit exceeded")
                        digest.update(chunk)
                        output.write(chunk)
                destination.chmod(0o444)
                after = os.fstat(stream.fileno())
                if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise IsolationError("Input changed during staging")
                hashes[relative.as_posix()] = digest.hexdigest()

    # Open every ancestor with O_NOFOLLOW; a path rename cannot swap our open
    # directory descriptors for an attacker-selected symlink during traversal.
    root_fd = os.open(source.anchor, flags | os.O_DIRECTORY)
    try:
        for part in source.parts[1:]:
            child = os.open(part, flags | os.O_DIRECTORY, dir_fd=root_fd)
            os.close(root_fd)
            root_fd = child
        walk(root_fd, Path())
    finally:
        os.close(root_fd)
    return hashes


def _bounded(docker, args, limit, seconds, cancelled):
    """Bound BOTH streams jointly, without communicate's unbounded buffering."""
    process = subprocess.Popen(
        [*docker.base, *args], env=docker.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    buffers = [bytearray(), bytearray()]
    lock, overflow = threading.Lock(), threading.Event()
    count = 0

    def drain(stream, index):
        nonlocal count
        try:
            while chunk := stream.read(65536):
                with lock:
                    left = max(0, limit - count)
                    buffers[index].extend(chunk[:left])
                    count += len(chunk)
                    if count > limit:
                        overflow.set()
        finally:
            stream.close()

    threads = [
        threading.Thread(target=drain, args=(stream, i), daemon=True)
        for i, stream in enumerate((process.stdout, process.stderr))
    ]
    for thread in threads:
        thread.start()
    deadline, reason = time.monotonic() + seconds, None
    try:
        while process.poll() is None:
            if cancelled and cancelled():
                reason = "cancelled"
            elif overflow.is_set():
                reason = "stream_limit"
            elif time.monotonic() >= deadline:
                reason = "timeout"
            if reason:
                process.kill()  # This stops the client; caller MUST remove the container.
                break
            time.sleep(0.05)
        process.wait(timeout=10)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        for thread in threads:
            thread.join(timeout=10)
    if overflow.is_set() and reason is None:
        reason = "stream_limit"
    return process.returncode, bytes(buffers[0]), bytes(buffers[1]), reason


def _extract(data, target, limit):
    """Never tar.extract: reject links/devices/traversal/duplicates and cap files."""
    hashes, total = {}, 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as archive:
        for entry in archive:
            path = PurePosixPath(entry.name)
            if path.is_absolute() or ".." in path.parts or "\\" in entry.name:
                raise IsolationError("Unsafe output path")
            if entry.isdir():
                continue
            if not entry.isfile() or entry.issparse():
                raise IsolationError("Output links, sparse files and special files are forbidden")
            name = path.as_posix()
            total += entry.size
            if total > limit or len(hashes) >= _MAX_FILES or name in hashes:
                raise IsolationError("Output byte/file limit or duplicate path")
            destination = target.joinpath(*path.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(entry) as stream, destination.open("xb") as output:
                shutil.copyfileobj(stream, output, length=65536)
            hashes[name] = _sha(destination)
    return hashes


def run_isolated(argv, inputs, output, policy, cancelled=None):
    """Run argv in containment and persist a result, including terminal failures.

    Preflight/invalid input errors raise IsolationError. Once a new output directory
    exists, all execution failures are also recorded in isolation.json. No result
    with unverified cleanup can be succeeded/cancelled.
    """
    policy = validate_policy(policy)
    if (
        not isinstance(argv, (tuple, list))
        or not argv
        or any(not isinstance(arg, str) or "\0" in arg for arg in argv)
    ):
        raise IsolationError("argv must be a nonempty string sequence without NULs")
    output = Path(output).absolute()
    if output.parent.resolve() != output.parent:
        raise IsolationError("Output parent must not contain symlinks")
    # Refuse outputs beneath inputs: staging would recursively include itself.
    if output == Path(inputs).resolve() or Path(inputs).resolve() in output.parents:
        raise IsolationError("Output must be outside inputs")
    output.mkdir(parents=True, exist_ok=False)
    result = {
        "schema": "quant.research-isolation-result/v1",
        "evidence_kind": "not_executed",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "policy": policy,
        "argv": list(argv),
        "seed": policy["seed"],
        "code_sha256": _sha(Path(__file__)),
        "collector_sha256": _sha(Path(__file__).with_name("isolation_collect.py")),
        "status": "blocked",
        "returncode": None,
        "inputs": {},
        "outputs": {},
        "cleanup_verified": None,
    }
    name = "quant-isolate-" + uuid.uuid4().hex
    result["container_name"] = name
    docker, attempted = None, False
    started = time.monotonic()
    try:
        result["environment"] = preflight(policy)
        docker = Docker()
        with tempfile.TemporaryDirectory(prefix="inputs-", dir=output) as staging:
            staging = Path(staging)
            staging.chmod(0o755)
            result["inputs"] = _stage(inputs, staging, policy["input_bytes"])
            if "," in str(staging):
                raise IsolationError("Docker mount paths cannot contain commas")
            if cancelled and cancelled():
                result.update(status="cancelled", cleanup_verified=True)
                return result
            attempted = True
            docker.call(
                "create",
                "--name",
                name,
                "--label",
                "quant.research-isolation=v1",
                "--read-only",
                "--network",
                "none",
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges=true",
                "--user",
                "65534:65534",
                "--pids-limit",
                str(policy["pids"]),
                "--memory",
                f"{policy['memory_mb']}m",
                "--memory-swap",
                f"{policy['memory_mb']}m",
                "--cpus",
                str(policy["cpus"]),
                "--ulimit",
                "nofile=256:256",
                "--ulimit",
                f"fsize={policy['output_bytes']}",
                "--init",
                "--no-healthcheck",
                "--ipc",
                "private",
                "--shm-size",
                "1m",
                "--log-driver",
                "none",
                "--mount",
                f"type=bind,src={staging},dst=/inputs,readonly,bind-recursive=disabled",
                "--tmpfs",
                f"/work:rw,nosuid,nodev,size={policy['work_mb']}m,mode=1777",
                "--tmpfs",
                f"/tmp:rw,nosuid,nodev,noexec,size={policy['tmp_mb']}m,mode=1777",
                "--workdir",
                "/work",
                "--env",
                "HOME=/tmp",
                "--env",
                "TMPDIR=/tmp",
                "--env",
                f"PYTHONHASHSEED={policy['seed']}",
                "--env",
                f"QUANT_RESEARCH_SEED={policy['seed']}",
                "--env",
                f"QUANT_IMAGE_ID={policy['image']}",
                "--entrypoint",
                "/usr/local/bin/python",
                policy["image"],
                "-I",
                "-c",
                "import time; time.sleep(172800)",
            )
            docker.call("start", name)
            result["evidence_kind"] = "container-execution"
            result["container_id"] = docker.inspect("inspect", name)[0]["Id"]
            code, stdout, stderr, reason = _bounded(
                docker,
                ["exec", name, *argv],
                policy["log_bytes"],
                policy["timeout_seconds"],
                cancelled,
            )
            (output / "stdout.log").write_bytes(stdout)
            (output / "stderr.log").write_bytes(stderr)
            result.update(
                returncode=code, status=reason or ("succeeded" if code == 0 else "failed")
            )
            if reason not in {"cancelled", "timeout", "stream_limit"}:
                # Docker cp's daemon mount view does not expose container tmpfs.
                # The trusted collector quiesces all other container processes
                # before archiving. No user process can race file replacement.
                collector = Path(__file__).with_name("isolation_collect.py").read_text("utf-8")
                code, archive, error, copy_reason = _bounded(
                    docker,
                    [
                        "exec",
                        name,
                        "/usr/local/bin/python",
                        "-I",
                        "-c",
                        collector,
                        str(policy["output_bytes"]),
                    ],
                    policy["output_bytes"] + 8 * 1024**2,
                    30,
                    cancelled,
                )
                if code or copy_reason:
                    raise IsolationError(
                        f"Output collection failed: {copy_reason or error[-1000:]}"
                    )
                artifacts = output / "artifacts"
                artifacts.mkdir()
                result["outputs"] = _extract(archive, artifacts, policy["output_bytes"])
                result["output_collection"] = "quiesced_container_regular_file_archive"
    except (
        IsolationError,
        OSError,
        ValueError,
        tarfile.TarError,
        subprocess.SubprocessError,
        KeyboardInterrupt,
    ) as exc:
        result.update(
            status="cancelled" if isinstance(exc, KeyboardInterrupt) else "failed",
            error=f"{type(exc).__name__}: {exc}",
        )
        if result["evidence_kind"] == "not_executed" and not isinstance(exc, KeyboardInterrupt):
            result["status"] = "blocked"
    finally:
        if docker is not None and attempted:
            try:
                docker.call("rm", "--force", "--volumes", name, check=False)
                remaining = docker.call("ps", "--all", "--filter", f"name=^/{name}$", "--quiet")
                result["cleanup_verified"] = not remaining.stdout.strip()
                if not result["cleanup_verified"]:
                    raise IsolationError("Container remains after removal")
            except (IsolationError, OSError, ValueError) as exc:
                result.update(
                    status="cleanup_failed", cleanup_verified=False, cleanup_error=str(exc)
                )
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        if docker is not None:
            docker.close()
        result["logs"] = {p.name: _sha(p) for p in output.glob("*.log")}
        _json(output / "isolation.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--inputs", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    try:
        policy = json.loads(args.policy.read_text("utf-8"))
        if args.check:
            print(json.dumps(preflight(policy), indent=2))
            return 0
        if not args.inputs or not args.output or not args.command:
            parser.error("--inputs, --output and command after -- are required")
        command = args.command[1:] if args.command[0] == "--" else args.command
        cancelled = threading.Event()
        previous = signal.signal(signal.SIGTERM, lambda *_: cancelled.set())
        try:
            result = run_isolated(command, args.inputs, args.output, policy, cancelled.is_set)
        finally:
            signal.signal(signal.SIGTERM, previous)
        print(json.dumps(result, indent=2))
        return 0 if result["status"] == "succeeded" else 2
    except (IsolationError, ValueError, OSError) as exc:
        parser.exit(2, f"Isolation refused: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
