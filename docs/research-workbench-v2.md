# 研究工作台第二阶段

从一份冻结配方执行训练、样本外回放、诊断与报告，再把已完成候选晋级为前向模拟账户。所有成交来自QExec账本，所有实验保留成功和失败尝试。

## 本机操作台

使用quant-workspace的research-workbench固定版本源码环境，以便验证全部仓库的代码身份和干净状态。`research`扩展声明完整应用依赖；仅安装一个脱离源码的wheel不能替代固定源码清单。从集成目录运行：

```powershell
python -m quant_pipeline.research_demo --advanced --output demos/advanced
python -m quant_pipeline.research_web --workspace studies/console --data-root demos --template demos/advanced/recipe.yaml --port 8766
```

打开http://127.0.0.1:8766。选择配方、保存、预检，再运行；修改已经登记的实验时，先改研究编号并点击“复制为新研究”。可勾选两至八项研究比较数据版本、代码身份、成本、区间与结果。失败实验不会从比较中消失。笔记和队列保存于SQLite；中断后需点击“运行／恢复”，不会隐式重启研究。

服务只监听本机，限定Host、请求来源、随机令牌、输入根目录及输出路径，不接受任意shell命令。一个工作区只有一个写进程；执行使用数据库冻结配方生成的独立请求和摘要校验。关闭服务会停止本服务启动的研究进程。不要将本机服务通过代理暴露到公网。

## 滚动样本外验证

```yaml
validation:
  method: walk_forward
  train_sessions: 80
  test_sessions: 40
  embargo_sessions: 5
  expanding: true
  selection_metric: total_return
  direction_policy: fixed
  direction_horizon: 5
  fdr_alpha: 0.05
```

`embargo_sessions`至少覆盖标签窗口。`train_ic`仅使用训练区间内成熟标签确定方向，至少两期有效标签。每折的`selection.json`在测试回放前保存方向及训练指标。只有base及显式variants参与训练选择，买入持有和压力诊断不参与选优。最后不足一个测试窗口的交易日计入unused_tail_sessions。

各折独立从相同初始现金开始，包含入场成本；拼接收益代表重复部署，不是连续持仓账户。主报告链接`validation.html`，提供逐折证据、候选与买入持有配对超额的移动区块bootstrap区间及BH调整。候选家族不完整时禁止输出家族级FDR结论。此类检验依赖平稳近似，只是历史探索证据，不保证独立留出或未来收益。

## 因子、组合和交易状态

```yaml
factor_expressions:
  risk_adjusted_momentum: momentum_20d / clip(volatility_20d, 0.005, 1)
factors:
  risk_adjusted_momentum: 1
allocation:
  mode: inverse_vol
  lookback: 20
  min_observations: 10
execution:
  mode: dynamic
  universe_field: member
  max_retry_sessions: 5
  status_fields:
    listed: listed
    delisted: delisted
    tradable: tradable
    limit_up: limit_up
    limit_down: limit_down
required_history:
  member: universe
  listed: status
  delisted: status
  tradable: status
  limit_up: status
  limit_down: status
```

`inputs.history`指向QDK导入后的历史目录。状态和成员按available_at/effective_at读取，缺历史就阻断，不能用今天的成分回填历史。证券主表也必须在当时开盘前已可用。因子表达式使用限定语法，无eval、属性访问或导入；函数及限制见quant-factors/docs/research-screening.md。

组合支持equal、inverse_vol、cost_aware，用当期NAV而非初始资金计算目标，保留权重、换手、约束与执行证据。资金和风控仍以可用现金为准。动态状态控制停牌、涨跌停、上市/退市和池进出，未成交订单按交易日有界重试。分红在除权日记录应收并进入NAV，在真实支付日转可用现金；区间外支付不会提前计入现金。

静态相关和残差增量不能替代样本外收益比较。可将原组合和新增因子的组合写成variants，保持相同数据、费用、验证窗口后比较。

## 前向模拟账户

```powershell
python -m quant_pipeline.research_paper promote studies/example/study.json --candidate base --output accounts/forward-01 --account-id forward-01 --start YYYY-MM-DD --end YYYY-MM-DD
python -m quant_pipeline.research_paper observe accounts/forward-01 --recipe recipes/updated-inputs.yaml --as-of YYYY-MM-DD
python -m quant_pipeline.research_paper seal accounts/forward-01
```

晋级要求源研究全部候选完成，源快照未变，数据覆盖注册前最近已收盘交易日，起点是已知的未来交易日且晚于开发区间。终点可以是周末等日历边界。策略、参数、代码和注册前数据前缀冻结；后续只接收同源、旧前缀不变的新快照。模型方向若使用train_ic，应先把方向明确写入新的固定配方研究再晋级。

带QDK版本化证据的主表按截至观察截止日的字段、版本可得时间及来源证据前缀冻结。重新核验后延长未来有效区间可以追加，但不能改变旧字段、旧可得时间或旧证据。没有这种证据结构的传统catalog继续按文件字节冻结。主表中的人工提取断言、附件哈希一致和附件语义被自动核验是不同层次，报告不能混用。

每次观察从账户注册起点连续重放，不每日重置现金。只接收已收盘的注册区间内交易日，校验旧账本收益前缀，保留失败尝试。末次快照的交易日历必须覆盖观察终点，收益覆盖全部注册交易日后才能一次性封存。数据库提交后派生文件写入失败可恢复同一份结果；已封存证据不能重新评价。代码升级或旧数据修订需要新账户。

账户提供日常回撤诊断和到期评价，没有经纪商连接，不会自动发送订单。软件测试使用合成数据与受控时钟，不代表真实未来观察已完成。

## 有据研究助手

`quant_agent.research_history`读取成功和失败研究，核验引用文件与哈希，输出相似研究、最小对照、缺失数据及边界。操作台直接调用同一入口。助手不会自动运行建议、改动费用/区间/留出，或把缺失来源包装成证据。离线检索可直接使用；在线模型需另行配置并明确启用，默认不发送研究文本。
