# Optional research isolation

`quant-isolate` and a legacy pipeline step's `isolation` field use the same
Linux Docker backend. This is an OS/container boundary. Existing host commands
and separate Python environments remain **trusted local execution**.

Supported hosts: Linux, a running local Docker daemon at
`unix:///var/run/docker.sock`, cgroup v2 with memory/swap/CPU/PID enforcement and
the built-in seccomp profile. Windows, Docker Desktop paths, remote daemons,
missing daemon permissions, disabled resource controls and images with declared
volumes are refused. No services are installed or started, no image is pulled
by the runner, and no host-process fallback exists.

An operator prepares a trusted Linux Python image with `/usr/local/bin/python`
and the research dependencies. Record its immutable ID using
`docker image inspect --format '{{.Id}}' IMAGE`. The runner accepts that
`sha256:...` ID only, not a movable tag. Image contents, Docker, the kernel and
the operator are trusted; this is not protection against their compromise.
Do not place credentials in the image. No host environment or Docker proxy
configuration is forwarded: each client uses a fresh empty config directory.

Save a policy, replacing the image ID with the actual 64 hex characters:

```json
{
  "schema": "quant.research-isolation/v1",
  "backend": "docker-linux",
  "image": "sha256:REPLACE_WITH_ACTUAL_IMAGE_ID",
  "memory_mb": 512,
  "cpus": 1,
  "pids": 64,
  "work_mb": 32,
  "tmp_mb": 32,
  "output_bytes": 16777216,
  "log_bytes": 1048576,
  "timeout_seconds": 60,
  "seed": 42
}
```

```bash
quant-isolate --policy policy.json --check
quant-isolate --policy policy.json --inputs frozen-inputs --output new-result -- \
  /usr/local/bin/python -I /inputs/research.py
```

API: `run_isolated(argv, inputs, output, policy, cancelled=None)`. The cancellation
callback returns a boolean. The CLI also handles SIGTERM and Ctrl-C. The output
directory must be new and outside inputs. Once created, it retains
`isolation.json` on failure, plus bounded stdout/stderr and safely collected
regular-file artifacts. Result statuses include `blocked`, `failed`, `timeout`,
`stream_limit`, `cancelled`, `cleanup_failed`, `succeeded`; only the last is a
successful execution. `not_executed` is not containment evidence.

Existing `quant-pipe run --config pipeline.yaml` entry:

```yaml
name: isolated-research
steps:
  - name: notebook-or-script
    cmd: [/usr/local/bin/python, -I, /inputs/research.py]
    inputs: frozen-inputs
    output: new-result
    isolation: # insert the policy mapping above
      schema: quant.research-isolation/v1
      backend: docker-linux
      image: sha256:REPLACE_WITH_ACTUAL_IMAGE_ID
```

Paths are relative to the step cwd. `cmd` must be an explicit argv list; shell
mode is rejected. Isolation is currently supported by this legacy step runner
and the standalone API/CLI, not the typed v2 DAG executor.

The image runs as UID/GID 65534 with no capabilities, no new privileges,
read-only rootfs, external network disabled, private PID/IPC namespaces and
bounded memory, no swap, CPU quota, PIDs, open files and per-file size. Frozen
regular inputs are copied with no-follow directory descriptors and mounted
read-only at `/inputs`. Mutable space is bounded tmpfs at `/work` and `/tmp`;
there is no writable host mount. Write retained artifacts beneath `/work`.
Combined stdout/stderr has a byte limit and kills an overflowing execution.
Outputs have a separate total byte/file limit and reject links, sparse files,
special files, duplicates and traversal. Oversized output causes failure, not
silent truncation. The container is paused for a stable output snapshot, then
force-removed with all descendants. Cleanup is verified via the daemon; an
unavailable daemon cannot certify cleanup. A host crash/SIGKILL can prevent
the supervisor's finally block; operators must inspect containers carrying
`quant.research-isolation=v1` after an abnormal host shutdown. The CLI/API does
not claim crash-resilient supervision.

The manifest records exact input/output SHA-256 maps, argv, policy, seed,
runner source hash, image ID, daemon/kernel identity, terminal state and cleanup.
`QUANT_RESEARCH_SEED` and `PYTHONHASHSEED` are supplied; arbitrary code must apply
the seed to its own libraries. Research result truthfulness is not certified by
containment. No model or market quality claim follows from these tests.

The `real research isolation` CI job runs file, host-visibility, network, cgroup,
memory, PID, tmpfs, output-link, log-flood, timeout and cancellation attacks on
actual Docker. It resolves a public test image tag to an exact local ID and
uploads that identity and per-run manifests. Local Windows tests certify only
refusal and validation, never Linux containment. The independent job needs no
private cross-repository token; existing full-stack CI remains separately required.

Docker references: [resource constraints](https://docs.docker.com/engine/containers/resource_constraints/),
[tmpfs lifecycle](https://docs.docker.com/engine/storage/tmpfs/),
[archive copy semantics](https://docs.docker.com/reference/cli/docker/container/cp/).
