# Factor Lab

Factor Lab 是一套面向 A 股的自适应量化研究操作系统。它关注的不是“找到一个永远有效的因子”，而是持续完成数据审计、假设登记、证伪、组合降险、休眠和恢复观察，并把每一步保存为可重演的研究证据。

> **证据边界：** 当前系统只支持历史研究与影子组合，不包含券商接口、真实下单或实盘切换。全部既有历史都属于已观察或 pseudo-OOS 数据；当前旧扩样本快照因 `st_history_unverified` 被阻断，只能用于交易引擎回归，不能用于候选晋级。系统冻结后的至少 60 个全新交易日，只是验证恢复能力的最低观察期，不是盈利证明。本项目不保证收益，也不构成投资建议。

## 当前主干

- PostgreSQL 16 是新 Research OS 的唯一运行事实源；Alembic 管理实验、试验账本、生命周期、恢复案例、运行和影子账本 schema。
- Tushare、AkShare 和本地文件通过统一 `SourceAdapter` 接入，数据按 PIT 双时态字段规范化并逐字段对账；争议、缺列、空历史 ST、乱码或哈希异常会阻断发布。
- MinIO 提供对象存储，PyIceberg 使用 PostgreSQL SQL catalog 管理 Gold 表、快照和不可变 tag；Parquet 是本机交换格式，Polars 与 DuckDB 已纳入 Research OS 运行时依赖，供查询层使用。
- 版本化研究合同和内容指纹绑定数据快照、因子或 Sleeve、组合政策、验证协议、唯一评估器和运行环境。新实验只允许 `research_os.long_only.v2`。
- 类型化因子 DSL、嵌套滚动验证、trial ledger、在线统计预算、Holm 校正、block bootstrap 和 Deflated Sharpe 共同限制选择偏差。
- 研究单位从单因子提升为 Sleeve；静态 Champion、受限动态 Challenger 和确定性状态机负责降权、休眠、观察与复活。
- 影子组合使用哈希链事件账本。信号在收盘后形成，只能在后续交易日使用当时可见的开盘价和交易状态执行；运行时拒绝 forward label。
- Dagster 提供日、周、月、季度任务和恢复 SLA sensor；仓库内置 `application_services:create_services` 作为完整操作桥，但仍要求一份显式 orchestration JSON。工厂、配置或输入缺失时任务会 fail-closed，不会伪造成功证据。
- FastAPI + Jinja WebUI 保留五项主导航：总览、研究、组合、运行、设置。活跃页面只读 PostgreSQL read-model，不回退扫描旧 SQLite 或 JSON。

完整设计、实现边界和运维说明见 [Research OS 架构与运行手册](docs/research-os-architecture.md)。

## 架构概览

```text
Tushare / AkShare / Local
          │
          ▼
Bronze 原始响应 → Silver PIT 规范化与多源对账 → Gold Iceberg 快照
                                                     │
                                                     ▼
预注册假设 → DSL / 沙箱插件 → nested walk-forward → Sleeve
                                                     │
                                                     ▼
PostgreSQL 账本 ← Champion / Challenger ← 健康监控与状态机
       │                                             │
       ├────────── WebUI read-model                  ▼
       └────────── Dagster                    事件式影子组合
```

## 项目结构

```text
src/factor_lab/research_os/       合同、数据面、研究方法、组合、账本和 CLI
src/factor_lab/webui_templates/   FastAPI/Jinja WebUI 模板
src/factor_lab/webui_static/      WebUI 样式
infra/research_os/                Compose、Dagster、Alembic 配置
configs/                          研究和兼容工作流配置
scripts/                          WebUI 与旧流程兼容入口
tests/                            Pytest 测试
docs/                             架构、运行手册和历史设计文档
artifacts/                        本地缓存与 legacy evidence；不应提交大型产物
```

## 安装

Research OS 本地开发环境建议安装基础包、基础设施依赖和测试依赖：

```powershell
python -m pip install -e ".[research-os,dev]"
```

只运行旧的轻量示例时可以使用 `python -m pip install -e .`，但这不包含 PostgreSQL、Iceberg 或 Dagster 依赖。

数据源凭证放在本地环境变量或未提交的 `.env` 中，例如 `TUSHARE_TOKEN`。不要向 Git 提交 API key、token 或数据库密码。

正式 orchestration 示例要求四个运行 profile：`primary-tushare`、`secondary-akshare`、`daily-crosscheck-local` 和 `pit-reference-local`。可从 `.env.example` 复制 profile ledger，再在 WebUI 设置页填写/测试；Tushare Token 可继续由 `TUSHARE_TOKEN` 提供，本地 profile 的根目录可覆盖配置模板中的默认根。后两个本地 profile 对应的逐日交叉核对文件和 PIT reference 文件不存在时，同步会按设计阻断，而不会把当前股票名录或空 ST 表当作历史证据。

## 本地 Research OS

先复制基础设施环境模板并修改本地密码：

```powershell
Copy-Item infra/research_os/.env.example infra/research_os/.env
```

`FACTOR_LAB_ORCHESTRATION_FACTORY` 必须指向一个可导入、返回 `ResearchOSServices` 的应用服务工厂。仓库已提供 `factor_lab.research_os.application_services:create_services`；它还要求 `FACTOR_LAB_ORCHESTRATION_CONFIG` 指向根据[安全配置模板](configs/research_os_orchestration.example.json)填写、且容器内可见的 JSON。若暂时只启动数据库、对象存储和 Dagster UI，可以清空 factory；调度任务会在缺少工厂、配置或必需输入时明确失败，而不是写入虚假结果。

启动单机栈：

```powershell
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml up --build -d
```

Compose 会启动 PostgreSQL、MinIO、Alembic migration job、Dagster webserver 和 daemon。默认端口为 PostgreSQL `15432`、MinIO API `9000`、MinIO Console `9001`、Dagster `8766`（容器内仍为 `3000`，避开本机保留端口）。`catalog-migrate` 会自动执行 `upgrade head`。

宿主进程需要把以下两个数据库变量设置为与 `infra/research_os/.env` 相同的 PostgreSQL URL：

```powershell
$env:RESEARCH_OS_DATABASE_URL = "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab"
$env:FACTOR_LAB_DATABASE_URL = $env:RESEARCH_OS_DATABASE_URL
$env:FACTOR_LAB_POSTGRES_PASSWORD_FILE = "H:/Program Data/factor-lab-runtime/secrets/postgres_password"
```

手工检查迁移并运行环境诊断：

```powershell
python -m alembic -c infra/research_os/alembic.ini current
python -m alembic -c infra/research_os/alembic.ini check
factor-lab doctor
```

启动 WebUI：

```powershell
python scripts/run_web_ui.py
```

访问 `http://127.0.0.1:8765/`；Dagster UI 位于 `http://127.0.0.1:8766/`。

更完整的 Compose 运维说明见 [本地基础设施手册](infra/research_os/README.md)。

## Research OS CLI

所有命令支持全局 `--database-url` 覆盖；生产/本地正式研究应使用 PostgreSQL，SQLite 只允许显式测试覆盖。

| 命令 | 作用 |
| --- | --- |
| `factor-lab data sync --spec source.json` | 探测数据源，将 Bronze Parquet 与 lineage metadata 内容寻址归档到 MinIO；本地文件仅作查询缓存。 |
| `factor-lab snapshot publish --spec snapshot.json` | 生成内容寻址快照、验证质量并登记到 catalog。 |
| `factor-lab research cycle --experiment experiment.json --data gold.parquet` | 使用唯一合同、DSL、walk-forward 和权威多头评估器运行一次研究周期。 |
| `factor-lab research freeze-epoch ...` | 单例冻结架构哈希；仅可用 accepted Gold 交易日历单向登记首个真正前瞻 session。 |
| `factor-lab monitor tick --input monitor.json` | 根据当时可见的健康观测更新 Sleeve 生命周期和恢复案例。 |
| `factor-lab shadow step --input shadow-step.json` | 原子提交一次后续交易日的影子成交、NAV 和持仓投影。 |
| `factor-lab legacy import --root artifacts` | 一次性登记旧 SQLite/JSON/Parquet 为只读 legacy evidence；默认封存旧 SQLite。 |
| `factor-lab doctor` | 检查依赖、PostgreSQL、MinIO 和本地路径，不改变研究状态。 |

查看参数：

```powershell
factor-lab --help
factor-lab research cycle --help
factor-lab shadow step --help
```

## WebUI 信息架构

| 主导航 | 路径 | 内容 |
| --- | --- | --- |
| 总览 | `/` | Champion、降险状态、数据健康、恢复 SLA 和最近运行。 |
| 研究 | `/research` | 假设谱系、实验、累计试验预算和否证结果。 |
| 组合 | `/portfolios` | Champion/Challenger、Sleeve 权重和影子 NAV。 |
| 运行 | `/runs` | Research OS 运行、失败信息、健康和数据血缘入口。 |
| 设置 | `/data-sources` | 数据源能力、连接状态与模型设置入口。 |

详情与低频信息保留在二级页面；`/cockpit` 和 `/dashboard-full` 重定向到总览，Hermes/Agent WebUI 路由不再提供。

## 测试与静态检查

```powershell
# Research OS 聚焦测试
python -m pytest tests -k research_os -q

# WebUI 的权威 read-model 与路由
python -m pytest tests/test_webui_research_os_read_model.py tests/test_webui_routes.py -q

# 全量测试
python -m pytest -q

# 不启动容器也可检查 Compose 展开结果
docker compose --env-file infra/research_os/.env `
  -f infra/research_os/docker-compose.yml config --quiet
```

需要真实 PostgreSQL 时，再运行 Alembic `upgrade head`/`check` 和 catalog smoke test。若 Docker daemon 未运行，Compose 配置检查仍可完成，但不能据此声称服务已经启动。

仓库内的 [Research OS CI](.github/workflows/research-os-ci.yml) 在 Windows 与 Linux 上运行完整测试和源码编译；独立的 Linux job 会实际启动 PostgreSQL、MinIO、Alembic、Dagster webserver/daemon，并执行 `doctor`。这条 CI 只验证工程与部署边界，不连接行情账户，也不构成任何市场证据。

## Legacy 兼容边界

旧 workflow/batch 实现、扩样本实现、SQLite 和 JSON 产物仍保留，用于数值回归、审计或一次性导入；但历史 daemon、worker、workflow/batch 脚本以及扩样本 `rounds/all` 命令均返回 `retired_legacy_entrypoint`，不会再写候选。新研究只能通过 Dagster 或 `factor-lab research cycle` 进入 `research_os.long_only.v2`、数据质量门和 lifetime trial ledger。

运行产物策略见 [artifact policy](docs/artifact-policy.md)。大型数据湖文件、数据库、缓存、诊断文件和生成结果不应提交。

## 免责声明

Factor Lab 是研究与工程实验系统。正确证伪、及时降险、保留失败证据并停止无效搜索，同样属于成功结果。历史回测、影子组合和 60 个新交易日观察都不能保证未来盈利，也不能替代投资、法律或合规建议。

## License

如果希望以特定开源协议公开发布，请在正式发布前添加 `LICENSE` 文件。
