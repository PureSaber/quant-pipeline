"""Trusted collector source executed via Docker exec, never via user input.

Docker's archive API does not expose tmpfs reliably. This collector runs inside
the PID/mount namespace, stops every other non-init process, then streams only
regular files. The host still treats the archive as untrusted and validates it.
"""

import os
import signal
import stat
import sys
import tarfile
import time
from pathlib import Path


def collect(limit):
    myself = os.getpid()
    deadline = time.monotonic() + 5
    while True:
        pids = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdecimal()} - {1, myself}
        for pid in pids:
            try:
                os.kill(pid, signal.SIGSTOP)
            except ProcessLookupError:
                pass
        live = {int(p.name) for p in Path("/proc").iterdir() if p.name.isdecimal()} - {1, myself}
        stopped = True
        for pid in live:
            try:
                state = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
                if state not in {"T", "t", "Z", "X"}:
                    stopped = False
            except FileNotFoundError:
                pass
        if live <= pids and stopped:
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Could not quiesce all payload processes")
        time.sleep(0.01)
    total, entries = 0, 0
    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
        for root, directories, files in os.walk("/work", followlinks=False):
            for name in sorted(directories + files):
                entries += 1
                path = Path(root) / name
                before = path.lstat()
                if entries > 4096 or len(path.parts) > 64:
                    raise ValueError("Output tree entry/depth limit exceeded")
                if stat.S_ISDIR(before.st_mode):
                    continue
                if not stat.S_ISREG(before.st_mode):
                    raise ValueError("Output links and special files are forbidden")
                total += before.st_size
                if total > limit:
                    raise ValueError("Output byte limit exceeded")
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, "rb") as stream:
                    after = os.fstat(stream.fileno())
                    if (before.st_dev, before.st_ino, before.st_mode, before.st_size) != (
                        after.st_dev,
                        after.st_ino,
                        after.st_mode,
                        after.st_size,
                    ):
                        raise ValueError("Output changed during collection")
                    entry = tarfile.TarInfo(path.relative_to("/work").as_posix())
                    entry.size = before.st_size
                    entry.mode = 0o644
                    archive.addfile(entry, stream)


if __name__ == "__main__":
    collect(int(sys.argv[1]))
