# Factor Lab

Factor Lab 是一个实验性的自动化量化因子研究与回测平台。它把数据准备、因子评估、稳健性检验、候选因子生命周期管理、自动研究守护进程和本地 FastAPI Web UI 组合在一起，用于持续探索、验证和管理量化研究想法。

> **项目状态：** 这是研究与工程实验系统，不是生产级实盘交易系统。本仓库中的任何内容都不构成投资建议。

## 主要功能

- 支持合成样例数据流程，便于快速本地验证。
- 支持基于 Tushare 的 A 股数据流程。
- 支持因子评估、时间切分稳健性检验、中性化、相关性/去重和组合 sanity check。
- 支持候选因子池、观察列表、淘汰区等研究生命周期管理。
- 使用 SQLite 保存实验记录、运行结果和报告。
- 支持自动化研究 daemon 与任务队列编排。
- 提供本地 Web UI，可查看健康状态、运行记录、LLM/provider 设置、Agent 角色和只读控制状态。
- 提供运行时加固与验证工具：
  - `scripts/verify_de_openclaw_runtime.py`
  - `scripts/build_queue_explanation.py`
  - `scripts/smoke_test_factor_lab.py`

## 项目结构

```text
src/factor_lab/                 核心 Python 包
configs/                        工作流与策略配置
scripts/                        命令行入口、运行脚本和维护工具
systemd/                        用户级 systemd service 模板
src/factor_lab/webui_templates/ FastAPI/Jinja Web UI 模板
tests/                          Pytest 测试
docs/                           计划、策略、运行手册和设计文档
artifacts/                      本地运行产物目录，不应提交大文件或运行态数据
```

## 安装

```bash
python3 -m pip install -e .
```

开发和测试环境可安装：

```bash
python3 -m pip install -e '.[dev]'
```

## 配置

从模板创建本地 `.env`：

```bash
cp .env.example .env
```

然后填写本地私有配置，例如：

```env
TUSHARE_TOKEN=replace-me
FACTOR_LAB_LLM_BASE_URL=https://example.com/v1
FACTOR_LAB_LLM_MODEL=replace-me
FACTOR_LAB_LLM_API_KEY=replace-me
```

真实 `.env` 文件、API key、token、password 等凭证不应提交到 Git。

## 常用命令

### 运行合成数据 smoke workflow

```bash
python3 scripts/run_first_workflow.py
```

### 运行 Tushare workflow

```bash
python3 scripts/run_tushare_workflow.py
```

### 启动本地 Web UI

```bash
python3 scripts/run_web_ui.py
```

默认访问地址：

```text
http://127.0.0.1:8765/
```

常用页面：

```text
/          轻量概览
/control   运行时控制与状态，只读展示
/health    健康检查与诊断
/runs      工作流运行记录
/agents    Agent 角色设置
/llm       LLM/provider 状态与设置
```

### 启动研究 daemon

```bash
python3 scripts/run_research_daemon.py
```

如需安装用户级 service，可执行：

```bash
./scripts/install_research_daemon_service.sh
```

## 验证

可运行以下聚焦测试和运行时检查：

```bash
pytest tests/test_verify_de_openclaw_runtime.py \
       tests/test_webui_routes.py \
       tests/test_daemon_heartbeat.py \
       tests/test_queue_explanation.py \
       tests/test_smoke_script.py -q

python3 scripts/verify_de_openclaw_runtime.py
python3 scripts/build_queue_explanation.py
python3 scripts/smoke_test_factor_lab.py
```

## 运行产物策略

`artifacts/` 是本地运行空间。大型生成结果、SQLite 数据库、Tushare 缓存、诊断文件、parquet feature store 和 generated candidate runs 都不应提交。

详见：

```text
docs/artifact-policy.md
```

如果需要示例数据，应优先使用 `tests/fixtures/` 下的小型确定性 fixture，或使用 `docs/snapshots/` 下经过整理的快照。

## OpenClaw 迁移说明

Factor Lab 目标是独立于旧 OpenClaw workspace 运行。项目可以保留 OpenClaw 风格的 Agent 角色架构思想，但运行时不应依赖 `/home/admin/.openclaw/workspace`。

相关文档和检查脚本：

```text
docs/openclaw-reference-policy.md
scripts/verify_de_openclaw_runtime.py
```

## 免责声明

本项目仅用于研究、工程实验和自动化量化研究流程探索，不保证收益，不构成投资建议，也不应直接作为实盘交易系统使用。

## License

如果希望以特定开源协议公开发布，请在正式发布前添加 `LICENSE` 文件。
