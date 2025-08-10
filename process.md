# 开发流程追踪（Process Log）

## 需求分析（第一性原理）
- 目标：构建面向 A 股量化研究与交易的数据与应用平台，具备稳定采集、去重、增量、日内/日间调度、统一存储、高性能读写、API 服务、前端可视化与检索、以及基于 LLM 的数据问答。
- 确定边界：以 AkShare 为主数据源，按需扩展；优先覆盖 A 股/ETF/指数/北向/融资融券/交易日历/宏观；支持分钟/日频；偏重研究与回测场景。
- 性能与稳定：ClickHouse 作为时序与明细存储；Parquet 作为冷存与归档；调度采用 APScheduler + Celery（后续可替换 Airflow）。

## 架构设计（KISS/DRY/SOLID/YAGNI）
- 采集层：`ak_ingest.lib` 模块化采集器（幂等、重试、限速、Schema 正规化）。
- 存储层：Parquet 分区落盘 + ClickHouse 分区表（按日期/标的）。
- 服务层（API）：FastAPI + Uvicorn，统一查询与任务触发；数据读 ClickHouse。
- 调度层：APScheduler（交易时段/非交易时段分开任务队列），支持断点续跑。
- AI 层：LangChain（工具：检索、SQL、指标/因子解释），后端组合工具；前端聊天与数据洞察。
- 前端：React + Ant Design + ECharts；页面：数据浏览、因子/行情可视化、作业监控、智能问答。

## 模块划分
- `ak_ingest/` 采集库与 CLI（已建立）
- `backend/` FastAPI + 调度 + ClickHouse 适配
- `frontend/` React Web 应用
- `deploy/` Docker Compose、ClickHouse 建表与初始化

## 数据库评估与结论
- 首选 ClickHouse：列式、向量化、MergeTree 分区、低延迟扫描、海量明细友好；支持物化视图做分钟→日聚合；支持去重（ReplacingMergeTree）。
- 辅以 Parquet：便于离线归档、跨语言共享、廉价存储；与 ClickHouse 相辅相成。

## 待办（TODO）
- [ ] deploy: ClickHouse 建表 SQL（equity_1d、etf_1d、northbound、margin、calendar、index_constitutes）
- [ ] backend: FastAPI 项目骨架，读 ClickHouse（HTTP 接口），提供查询与任务触发 API
- [ ] backend: 调度器（APScheduler），交易/非交易窗口内的任务编组
- [ ] backend: LangChain 工具化（SQLDatabaseChain + 自定义 Tool：ak_ingest 触发）
- [ ] frontend: React 脚手架与页面结构（登录占位、数据表格、图表、作业监控、LLM 聊天）
- [ ] ingest: ClickHouse 写入器（批量、去重写入，支持 ReplacingMergeTree）
- [ ] ingest: A 股分钟线与日线的统一 Schema、增量策略、断点续跑
- [ ] docs: README 与开发手册扩展（运行、调试、扩展接口规范）

## 已完成（DONE）
- [x] 采集框架雏形：YAML 配置驱动、列重命名、分区 Parquet 落盘、指数成分/ETF/北向/融资融券/日历采集
- [x] 依赖安装与快速验证；输出样本数据
- [x] 文档：README 更新、数据目录 CSV（初稿）

## 测试
- 单元：采集模块函数级（safe_ak_call、normalize、write_partitioned_parquet）
- 集成：小窗口采集并比对分区与行数；ClickHouse 导入校验（后续）

## 风险与对策
- AkShare 接口波动：容错重试、字段兜底、保存 raw 副本
- 源站限流：随机停顿、任务分片、夜间批量
- 数据漂移：Schema 校验与报警、物化视图校对