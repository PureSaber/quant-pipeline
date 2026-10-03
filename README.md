# quant-pipeline

只读查看已登记的前向账户：

```sh
python -m quant_pipeline.research_paper inspect /path/to/account
```

`inspect`核验保存的登记定义、登记时间及全部完成观测的产物哈希，输出结构化JSON。
它不初始化数据库、不重建报告、不刷新行情，也不执行策略或登记新账户。
没有观测时绩效为不可用；失败尝试保留。核验范围是已保存证据的一致性，不是当前输入
新鲜度或实盘有效性认证。查看历史冻结账户可使用单独的维护环境，账户原执行环境保持不变。

研究可信度升级：接口、使用示例、验收及限制见 [11–20 使用说明](docs/RESEARCH_INTEGRITY_11_20.md)。

Deterministic local orchestration for research, backtest, and paper-trading workflows across the
PureSaber quant stack. Version 0.4.0 retains the typed `schema_version: "2.0.0"` DAG and adds a
quiet-by-default close-of-day notification contract. Generated full/core coverage evidence is
explicitly ignored locally. The existing v1 linear YAML behavior remains available.

For close-of-day runs, configure `alerts_file` to the report hub's generated alert sidecar. The
pipeline verifies that sidecar and atomically writes `notification-latest.json`. Its `notify` flag
is false for a healthy run with information-only alerts, and true for failed/blocked runs or
critical/warning alerts.

## Install

For the current cross-repository research workbench, use the verified
[quant-workspace research profile](https://github.com/PureSaber/quant-workspace/tree/main/profiles/research-workbench).
Its bootstrap and verifier install exact source commits and the closed external dependency lock.
The package's existing Git requirements describe the earlier release closure; installing the
`research` extra alone is not a substitute for this source profile. Coordinated release versions
and tags remain a separate release step.

```bash
pip install --no-deps -r requirements.lock
pip check
pip install -e . --no-deps --no-build-isolation
pip check
```

Continuous walk-forward selected-path results are now published only after complete execution,
return-date coverage checks and durable evidence writes. Resume validates the summary checksum,
the complete artifact inventory, frozen selections, and metrics recomputed from verified returns.
Failed attempts remain in sibling `selected-continuous-attempt-*` directories with diagnostics;
an interrupted legacy directory without its final summary is preserved before retry. Completed
legacy caches without the new integrity record fail closed: rerun in a new study output directory,
leaving the old evidence intact. Upstream training and execution limitations are retained.
The cache check, recovery, execution and publication share a cross-process lock in a stable
sibling directory. A competing invocation exits with a lock error and can be retried after the
active run finishes; it never archives or replaces the active/completed output.

Rebuild the complete runtime, development, and editable-build lock with Python 3.10 so the oldest
supported interpreter's conditional dependency closure remains explicit:

```bash
python -m piptools compile --extra dev --build-deps-for editable \
  --allow-unsafe --strip-extras --resolver backtracking \
  --index-url https://pypi.org/simple --output-file requirements.lock pyproject.toml
```

## V1 compatibility

```bash
quant-pipe run --config configs/daily_paper.yaml --dry-run
quant-pipe run --config configs/daily_paper.yaml
```

For the research-integrity post-run gate, set the completed run ID and the point-in-time asset
return file before executing the standard-contract validation, attribution, and experiment indexing
steps:

```powershell
$env:QUANT_RUN_ID = "run_001"
$env:QUANT_ASSET_RETURNS = "D:/quant_data/asset_returns.csv"
quant-pipe run --config configs/research_integrity_postrun.yaml
```

Pipeline configs reference `quant-workspace` env vars such as `{QW_QUANT_LAB_REPO}`.

V1 string commands and opt-in `shell: true` remain available only through the legacy configuration
shape. Historical configs are not rewritten.

## Typed DAG v2

V2 requires stable step and artifact IDs, explicit dependencies, argv commands, retry and timeout
policies, and paths contained by `workspace_root`. Shell execution is rejected. The full structural
schema is at `configs/schema/pipeline-v2.schema.json`; semantic validation additionally rejects
duplicate or unknown IDs, self-dependencies, cycles, producer conflicts, undeclared artifact
dependencies, output path conflicts, and runtime path escape.

```yaml
schema_version: "2.0.0"
name: example
workspace_root: "."
checkpoint_path: ".state/checkpoint.json"
log_dir: ".state/logs"
fail_fast: false
artifacts:
  - artifact_id: result
    path: artifacts/result.txt
    producer: build
    required: true
    immutable: true
steps:
  - id: build
    kind: research
    needs: []
    command: [python, -m, research_job, --out, artifacts/result.txt]
    inputs: []
    outputs: [result]
    retry:
      max_attempts: 2
      retry_exit_codes: [75]
      retry_exceptions: [TimeoutExpired]
      backoff_seconds: 1
    timeout: 300
```

Run and strictly resume with an immutable `StackManifest 1.0.0`:

```bash
quant-pipe run --config pipeline-v2.yaml --stack-manifest stack-manifest.json --run-id run-001 --seed 7
quant-pipe run --config pipeline-v2.yaml --stack-manifest stack-manifest.json --run-id run-001 --seed 7 --resume
```

The runner uses lexicographically deterministic topology ordering. Failed descendants become
`blocked`; independent branches continue unless `fail_fast` is enabled. Retries match only the
configured process exit codes or executor exception names. Steps whose `kind` is `data_quality`,
`schema_validation`, `sequence_validation`, `hash_validation`, or `pit_validation` are fail-closed
gates: their failures never retry even if a retry matcher is configured. Contract, path, and
artifact hash failures also never retry.

Each attempt has separate immutable stdout/stderr logs and SHA-256 values. Checkpoints are canonical
JSON, self-hashed, fsynced, and atomically replaced after state transitions. Resume verifies the run
ID, config hash, stack manifest hash, seed, event sequence, attempt logs, idempotency keys, and every
immutable output hash. This validation also applies to a pending retry checkpoint before any resume
event or replacement checkpoint is written. Missing or modified inputs, outputs, or logs fail
closed.

The v2 implementation accepts only a canonical, self-hashed, release-ready `StackManifest 1.0.0`
produced by the published `quant-workspace v0.3.1` contract. The dependency uses the immutable
annotated tag `v0.3.1`, which peels to commit
`537388a4d9548b612fa1e4b306c482c04b45c433`. File inputs must use canonical JSON;
both files and mappings are validated by `quant-workspace`. The integration is a required runtime
dependency and is pinned in `requirements.lock`.

The 0.4.0 change does not alter DAG, checkpoint, retry, integrity, or v1 compatibility semantics.
To roll back the candidate, revert the 0.4.0 commit and rebuild from the reverted
`pyproject.toml`. Never move or recreate any historical tag.

This package does not provide distributed scheduling, network execution, credentials, or live order
submission.

## Related

- [quant-workspace](../quant-workspace)
- [quant-lab](../quant-lab)
# 研究工作台第二阶段

本机操作台、自动滚动样本外验证、限定因子表达式、当前NAV组合配置、动态交易状态、前向模拟账户和历史研究助手已接入。使用quant-workspace提供的固定源码集成环境，完整命令与验收边界见[工作台指南](docs/research-workbench-v2.md)。

CI的`requirements-research.lock`固定完整研究依赖，应用依赖固定到Git提交；安装后运行`pip check`。`requirements.lock`保留基础编排环境。基础或研究依赖变化时，必须更新对应声明与锁；测试会核验`requirements-research.in`中的版本约束和直接下载地址是否进入研究锁。用Python3.10重建研究锁：

```bash
python -m piptools compile --extra dev --extra research --build-deps-for editable \
  --allow-unsafe --strip-extras --index-url https://pypi.org/simple \
  --constraint requirements-research.in --output-file requirements-research.lock pyproject.toml
```

quant-workspace的`stack.json`与公共依赖锁共同定义独立的集成快照。更新该快照时，应在集成PR中一起修改并通过跨仓验收；单仓依赖升级不会自动修改冻结研究环境。

## 研究可信度 11–20

新增跨研究家族 DSR/CSCV/SPA/MCS 入口、依赖 bootstrap/HAC，以及训练期决定参数的连续样本外账户。
`account_policy` 默认仍为 `independent`；连续账户需显式选择，且不能拼接独立资金账户来冒充连续选择路径。
本批联合锁文件保留报告端的固定公共包约束，并增加经验证的 arch/statsmodels 组合与新应用提交。
使用方法、锁文件重建、嵌套验证入口及限制见 [研究可信度指南](docs/RESEARCH_INTEGRITY_11_20.md)。
