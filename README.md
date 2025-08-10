AkShare 数据采集脚手架

快速开始
1) 安装依赖（建议国内镜像）：
   pip install -U pip && pip install -r /workspace/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

2) 运行采集（示例：上证50成分的 A 股日线前复权 + 510300 ETF 日线）：
   python /workspace/ak_ingest/ingest.py --config /workspace/ak_ingest/config.yaml --start 20180101 --end 20201231 --output-root /workspace/data

输出规范
- Parquet 分区布局，示例：
  - /workspace/data/ak_parquet/prices_equity_1d/dataset=equity_a_daily_qfq/adjust=qfq/year=2020/symbol=600519.SS.parquet
  - /workspace/data/ak_parquet/reference/index_constituents/index_code=000016/*.parquet

注意
- 首次运行会从 AkShare 源站抓取数据，请合理限速。脚本内已做基础重试与限速。
- AkShare 接口与字段可能变动，配置中提供列重命名，可按需调整。
- 更多数据集可在 `config.yaml` 中按已有样式扩展。
