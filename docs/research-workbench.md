# 单配方研究入口

`python -m quant_pipeline.research_workbench recipe.yaml --output ../studies/example`

代码必须来自干净Git检出。配方和输入先冻结，再由quant-lab预登记全部候选；A股/ETF交给a-share-multifactor的QExec适配器，完成后由quant-report-hub发布research.html。重复命令恢复已有结果，失败重试保留历史；同study_id修改定义会拒绝。退出码2表示存在失败候选。

合成安装验证：`python -m quant_pipeline.research_demo --output ../synthetic-etf --asset etf`，再运行生成的recipe.yaml。合成结果不作为真实策略收益。

期货/加密必须加`--fixture-python ../.venv-fixtures/Scripts/python.exe`。子进程核对QDK v0.8.1、QExec v0.5.1、QLab v0.3.1的实际VCS提交，主进程绝不替换这套冻结认证依赖。子进程清空继承的PYTHONPATH，避免当前源码污染冻结环境。完整安装和四类模板见quant-workspace的`profiles/research-workbench`。
