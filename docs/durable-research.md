# 独立研究Worker（源码集成候选v1）

多候选原子提交、共享输入、整批取消和保留历史的显式重试见
[Durable research batches](durable-batches.md)。批次复用本页既有 Worker 和尝试语义。

`durable_jobs.JobStore`提供本机SQLite持久队列、逐次尝试、租约、取消和结构化事件。`durable_worker.Worker`在独立子进程运行管理员登记的可信处理器。提交客户端退出不停止任务；Worker异常退出后，另一Worker在租约到期时回收尝试。现有DAG、Studio Notebook及前向账户入口保持原语义；本功能需显式启用。

## 运行

基础队列只依赖Pipeline既有环境。研究处理器额外需要提供`quant_data_kit.artifacts/shared_snapshots`、`quant_factors.cache`和`quant_lab.rolling_research`的候选版本。当前仍为跨仓源码候选，既有固定Git依赖不会自动包含这些接口；集成验收显式将四仓`src`放入`PYTHONPATH`并记录来源。正式发布需要按仓库原有流程生成新依赖锁及发布候选。

可信本地配置示例（PowerShell）：

```powershell
$env:QUANT_RESEARCH_STORE_ROOT = 'D:\research-v1\cas'
$env:QUANT_RESEARCH_CACHE_DB = 'D:\research-v1\factor-cache.db'
python -m quant_pipeline.durable_client --database D:\research-v1\jobs.db submit --research --request request.json --key project-run-candidate
python -m quant_pipeline.durable_worker --database D:\research-v1\jobs.db --workspace D:\research-v1\attempts --handler rolling_research=quant_lab.rolling_research:worker_handler --artifact-guard quant_pipeline.durable_research:retain_artifacts
```

客户端与Worker是两个独立进程。运行多个Worker可竞争领取；首版数据库和CAS须位于同一本机可靠文件系统。`--once`领取并处理一次后退出；没有匹配任务也退出。持续进程空队列每250ms检查一次。应由本机服务管理器按需启动并管理进程；本功能不会修改或启动已有服务。

CLI的`get JOB_ID`、`list --status running`、`attempts JOB_ID`、`events JOB_ID`返回JSON；`cancel JOB_ID`提交取消。失败状态由返回JSON中的status及持久记录表示，Worker退出码0仅表示Worker命令正常完成，不能据此把研究判为成功。

## 请求和处理器

```json
{
  "schema_version": "quant.job-request/v1",
  "kind": "rolling_research",
  "project_id": "synthetic",
  "run_id": "window-5",
  "payload": {},
  "inputs": [],
  "timeout_seconds": 120,
  "max_attempts": 2
}
```

这里的空payload/inputs只是字段结构；真实研究必须提供Lab完整`quant.rolling-research/v1`payload，并将payload.snapshot加入inputs。Lab的`examples/rolling_research_fixture.py`生成完整合成快照和三组请求。处理器签名为`handler(request: dict, workspace: Path) -> {"result": dict, "outputs": list[ArtifactRef]}`。任务不能指定模块、shell或任意本地路径；kind仅匹配启动时的登记项。

每次执行拥有独立attempt目录，保存request.json、context.json、stdout.log、stderr.log和result.json。context包括job_id、attempt_id、fencing_token、worker_id、project_id和run_id。result协议限制1MiB且必须是有限JSON；大产物写CAS并返回引用。可信研究代码仍有当前OS用户权限，此机制不是不可信代码沙箱，也没有硬内存/CPU限额。

子进程的Python日志以逐行JSON写入stderr.log，保留组件LogRecord.extra字段、已有JSON事件和异常堆栈，并附加可信context.json中的任务/尝试身份。数据组件的时间戳、上下文和查询统计不会被普通文本formatter丢弃。处理器直接print的内容仍进入stdout.log。

## 状态、重试与时间

|事件|前态|后态及约束|
|---|---|---|
|submit|无|queued；同键同完整规范请求返回同job，同键异请求拒绝|
|claim|queued|running；新attempt_id，fencing_token递增|
|heartbeat|running|仅当前未到期attempt可续租|
|finish成功|running|succeeded；租约和token在事务内校验|
|finish失败|running|允许重试且次数未耗尽时queued，否则failed|
|租约到期后claim|running|旧attempt记expired，允许重试则重新queued|
|cancel|queued|cancelled，永不执行|
|cancel|running|记录cancel_requested；Worker停进程后确认cancelled|

SQLite的BEGIN IMMEDIATE事务决定先后。max_attempts包括首次领取、失效及失败尝试。处理器失败和超时按声明次数重试；取消不重试。同库幂等键是全局作用域，完整请求按键排序、保留数组顺序、拒绝NaN/Infinity；不同project/run或1与1.0均不是相同请求。计算缓存键与提交幂等键彼此独立。

重复或过期finish返回LeaseLost，不产生第二次完成事件；不论结果是否相同，都应get查询权威结果。取消先提交会禁止后续成功；成功先提交时，取消返回既有成功。租约过期使用同机UTC Unix秒，执行超时使用monotonic；大幅调时可能影响恢复时刻。任务库事件timestamp也是UTC Unix秒。

正常超时/取消时先终止进程树再提交终态；突然杀死Worker可能遗留研究子进程，它可以完成计算但没有任务库提交权。attempt目录彼此隔离，旧token无法覆盖新结果。租约到期事件明确process_termination_confirmed=false，因此逻辑cancelled不等于确认孤儿进程已结束。可信处理器不得启动脱离生命周期的后台任务。外部副作用必须自行设计幂等，本系统不承诺exactly-once执行。

## 引用与故障窗口

研究提交适配器在排队前retain inputs。Worker在执行前再次retain inputs，在结果成功前retain outputs；QDK快照引用保护递归成员。研究处理器发布产物时已带owner pin。CAS发布/保留成功后才允许任务库完成事务，失败或崩溃可能保留多余pin，不会自动release。此保守策略需要后续由审计保留政策回收，不能在Worker失败回调直接释放共享输入。

JobStore是存储层API，直接调用finish只校验ArtifactRef结构；具有写库权限的管理员绕开Worker就承担内容核验责任。CLI研究入口及Worker通过真实QDK适配器做核验和引用保护；缺少artifact_guard时，有输入/输出引用的任务明确失败。

## 验证

```text
pytest -q tests/test_durable_jobs.py tests/test_durable_worker.py tests/test_durable_client.py
pytest -q tests/test_durable_research_integration.py
```

第一组覆盖真实多进程竞争、幂等、租约、取消竞态、重复终态、子进程超时、失败重试和Worker死亡恢复。第二组必须在四仓候选接口实际可导入时执行，否则会明确skip；覆盖三组参数、共享快照、缓存命中、真实失败及恢复，保留integration-evidence.json，不能把skip计作通过。
