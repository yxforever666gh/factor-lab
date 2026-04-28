# Factor Lab 迁移完成报告

## 迁移时间
2026-04-24 16:20

## 迁移内容

### 1. 物理迁移
- ✅ 从 `/home/admin/.openclaw/workspace` 迁移到 `~/factor-lab`
- ✅ 复制了 13GB 数据（src, scripts, tests, artifacts, configs 等）
- ✅ 保留了所有历史数据和缓存

### 2. LLM 配置迁移
从 ccswitch/Hermes 配置中提取了三个 LLM provider：

**Option 1: ccvibe (Claude Opus 4-7) - 当前激活**
- Base URL: https://ccvibe.vip
- Model: claude-opus-4-7
- API Key: sk-10a5b46...8491

**Option 2: nowcoding (GPT-5.4)**
- Base URL: https://nowcoding.ai/v1
- Model: gpt-5.4
- API Key: sk-7nGbfu0...FnHY

**Option 3: ai-continue (GPT-5.4)**
- Base URL: https://rayplus.site
- Model: gpt-5.4
- API Key: sk-a49732a...ecb7

### 3. 服务配置
- ✅ WebUI 服务运行在 http://127.0.0.1:8765/
- ✅ Systemd 服务已配置并启用自启动
- ✅ 工作目录: /home/admin/factor-lab
- ✅ Python 包已安装（editable mode）

### 4. 环境配置
- ✅ `.env` 文件已配置完整的 API keys
- ✅ Provider 设置为 `real_llm`（通用 LLM provider）
- ✅ 文件权限设置为 600（安全）

## 当前状态

### 运行中的服务
- factor-lab-web-ui.service (PID: 698577)

### 配置文件
- ~/factor-lab/.env - 主配置文件
- ~/.config/systemd/user/factor-lab-web-ui.service - Systemd 服务

### 测试结果
- 226 个测试正常运行
- LLM 配置正确加载
- Provider: real_llm, Model: claude-opus-4-7

## 下一步

### 待实现功能
1. WebUI Settings 页面 - LLM 配置管理界面
2. WebUI Control 页面 - 守护进程控制界面
3. 配置测试连接功能
4. 日志查看功能

### 切换 Provider
编辑 `~/factor-lab/.env`，注释当前 provider，取消注释想要使用的 provider：

```bash
# 切换到 nowcoding
# FACTOR_LAB_LLM_BASE_URL=https://ccvibe.vip
# FACTOR_LAB_LLM_MODEL=claude-opus-4-7
# FACTOR_LAB_LLM_API_KEY=sk-10a5b46...8491

FACTOR_LAB_LLM_BASE_URL=https://nowcoding.ai/v1
FACTOR_LAB_LLM_MODEL=gpt-5.4
FACTOR_LAB_LLM_API_KEY=sk-7nGbfu0...FnHY
```

然后重启服务：
```bash
systemctl --user restart factor-lab-web-ui.service
```

## 总结
✅ Factor Lab 已成功从 OpenClaw workspace 迁移到独立目录
✅ 三个 LLM provider 配置已从 ccswitch 导入
✅ 系统可独立运行，不再依赖 OpenClaw
