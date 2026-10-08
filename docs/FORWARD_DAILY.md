# 前向研究日常流程

`quant-forward`为已登记的`research_paper`账户提供一个显式、手动的日常入口。它只负责核验和接线：真正的观察、连续账户重放、失败事件、同账户互斥和结果哈希仍由原生`research_paper.observe`、`TrialRegistry`和`study_lock`完成。

本入口不创建研究、不晋级候选、不修改登记定义、不封存账户、不连接券商，也不包含调度器。旧自动化保持原状态。`status`和`plan`绝对只读；`run`才可能产生一次原生观察尝试，并把编排回执写到显式的账户外目录。

## 配置契约

结构定义见[`configs/schema/forward-daily-v1.schema.json`](../configs/schema/forward-daily-v1.schema.json)，暂停的合成示例见[`examples/forward-daily.synthetic.paused.yaml`](../examples/forward-daily.synthetic.paused.yaml)。路径相对配置文件解析。

```yaml
schema_version: quant.forward-daily/v1
approval: approved
account: ../accounts/forward-01
account_id: forward-01
definition_sha256: <account.json中的64位定义哈希>
input_recipe: ../recipes/forward-01-inputs.yaml
input_recipe_sha256: <该YAML文件的SHA-256>
receipt_dir: ../forward-receipts
market:
  timezone: Asia/Shanghai
  session_close: "15:00"
  calendar_source: input_bundle
```

当前原生前向账户只实现中国市场的`Asia/Shanghai`、15:00收盘边界，所以v1要求显式写出并精确匹配这一契约。以后支持其他市场时必须先扩展原生账户契约；工作流不会猜测时区或收盘时间。`approval: approved`只是操作开关，不是独立的研究真实性认证。

输入YAML除`inputs`引用外必须与账户冻结配方完全一致，其文件字节也必须匹配`input_recipe_sha256`。工作流还会核验账户ID、定义哈希、源研究哈希、源定义、候选、完整原生代码身份、输入域、数据血缘、登记前缀和已观察前缀。资料变化若不满足原生追加规则会阻断，而不会重设基线。

输入bundle的manifest必须提供带时区的`captured_at`。日历必须覆盖运行日和采集日，目标日必须由该日历明确列为交易日，而且必须同时是采集时刻及运行时刻最近已收盘的交易日。目标日的raw、adjusted和benchmark数据必须存在。缺少这些证据返回`data_missing`；工作日从不被默认当成交易日，旧快照也不能补写为新的前向事实。

## CLI和状态

三个命令都要求显式给出目标交易日；生产CLI没有可回拨时钟参数。

```powershell
quant-forward status --config configs/forward-01.yaml --as-of 2026-10-08
quant-forward plan   --config configs/forward-01.yaml --as-of 2026-10-08
quant-forward run    --config configs/forward-01.yaml --as-of 2026-10-08
```

`status`和`plan`输出JSON并返回0，只读核验账户、保存产物、配置、输入和日历。主要`state`如下：

| state | 含义 | `run`行为 |
| --- | --- | --- |
| `ready` | 所有门禁通过 | 调用原生observe一次 |
| `completed` | 已验证的原生观察已覆盖目标日 | 不重复观察或记账 |
| `market_closed` | 受覆盖的交易日历明确没有该交易日 | 阻断 |
| `not_due` | 显式收盘时刻尚未到达 | 阻断 |
| `data_missing` | 日历、采集时间、账户或当日数据证据缺失 | 阻断 |
| `running` | 未终结attempt且原生账户锁仍被持有 | 阻断 |
| `interrupted` | 未终结attempt但锁已释放 | 仅显式run可按原生契约恢复 |
| `retryable_failure` | 原生失败事件已保留 | 仅显式run可重试 |
| `verification_failed` | 哈希、代码、血缘、前缀或产物核验失败 | 阻断 |
| `historical_backfill_blocked` | 目标日不是采集及运行时刻最近已收盘会话 | 阻断 |
| `paused`、`sealed`、`outside_window` | 操作开关关闭、账户已封存或越界 | 阻断 |

`run`成功返回0，门禁阻断返回3，执行失败返回5，配置错误返回2。它不自动重试。若进程在原生attempt开始后中断，下一次`status/plan`根据追加事件及账户锁区分`running`和`interrupted`；下一次显式`run`才让原生契约把孤立attempt标为`interrupted`并开始新attempt。

每次`run`都会在`receipt_dir/<account_id>/<as_of>/`追加`quant.forward-daily-receipt/v1` JSON。回执绑定编排配置及源码、账户定义、源研究、候选、原生代码、输入身份、采集时间，以及成功时的原生attempt和结果哈希。预检阻断和执行失败也有回执。回执目录不得位于冻结账户目录内；`status/plan`不会创建它。

## 合成验收边界

测试用`research_demo`合成行情、受控时钟和受控账本覆盖交易日、采集时间、同日幂等、失败保留、显式重试、进程中断、同账户并发及资料变化。它只证明软件契约，不表示任何真实未来观察已经发生。将示例改成`approved`前，必须先用`research_paper promote`生成新的合成账户，填入真实定义哈希和输入YAML哈希；本仓库不会自动执行这一步。
