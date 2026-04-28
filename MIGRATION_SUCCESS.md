# ✅ Factor Lab 迁移成功

## 迁移完成时间
**2026-04-24 16:21 CST**

---

## 📦 迁移内容

### 1. 物理迁移
- ✅ **源目录**: `/home/admin/.openclaw/workspace`
- ✅ **目标目录**: `~/factor-lab`
- ✅ **数据量**: 约 13GB
- ✅ **内容**: 源码、测试、配置、缓存、历史数据

### 2. LLM 配置迁移
从 **ccswitch/Hermes** 配置中成功提取并配置了三个 LLM provider：

#### 🟢 Option 1: ccvibe (Claude Opus 4-7) - **当前激活**
```
Base URL: https://ccvibe.vip
Model: claude-opus-4-7
API Key: sk-10a5b46...8491 ✓
```

#### ⚪ Option 2: nowcoding (GPT-5.4)
```
Base URL: https://nowcoding.ai/v1
Model: gpt-5.4
API Key: sk-7nGbfu0...FnHY ✓
```

#### ⚪ Option 3: ai-continue (GPT-5.4)
```
Base URL: https://rayplus.site
Model: gpt-5.4
API Key: sk-a49732a...ecb7 ✓
```

### 3. 服务配置
- ✅ **WebUI 地址**: http://127.0.0.1:8765/
- ✅ **Systemd 服务**: `factor-lab-web-ui.service`
- ✅ **自启动**: 已启用
- ✅ **工作目录**: `/home/admin/factor-lab`
- ✅ **Python 包**: 已安装（editable mode）

### 4. 环境配置
- ✅ **配置文件**: `~/factor-lab/.env`
- ✅ **Provider**: `real_llm` (通用 LLM provider)
- ✅ **API Keys**: 完整配置，权限 600
- ✅ **测试**: 226 个测试通过

---

## 🎯 当前状态

### 运行中的服务
```bash
● factor-lab-web-ui.service - Factor Lab Web UI
  Active: active (running)
  PID: 699251
  URL: http://127.0.0.1:8765/
```

### 配置文件位置
- **主配置**: `~/factor-lab/.env`
- **Systemd 服务**: `~/.config/systemd/user/factor-lab-web-ui.service`

### 验证结果
- ✅ LLM 配置正确加载
- ✅ Provider: real_llm
- ✅ Model: claude-opus-4-7
- ✅ Base URL: https://ccvibe.vip
- ✅ API Key: 已配置

---

## 🔄 如何切换 Provider

编辑 `~/factor-lab/.env`，注释当前 provider，取消注释想要使用的 provider：

### 切换到 nowcoding (GPT-5.4)
```bash
# 注释掉 ccvibe
# FACTOR_LAB_LLM_BASE_URL=https://ccvibe.vip
# FACTOR_LAB_LLM_MODEL=claude-opus-4-7
# FACTOR_LAB_LLM_API_KEY=sk-10a5b46...8491

# 启用 nowcoding
FACTOR_LAB_LLM_BASE_URL=https://nowcoding.ai/v1
FACTOR_LAB_LLM_MODEL=gpt-5.4
FACTOR_LAB_LLM_API_KEY=sk-7nGbfu0...FnHY
```

### 切换到 ai-continue (GPT-5.4)
```bash
# 注释掉 ccvibe
# FACTOR_LAB_LLM_BASE_URL=https://ccvibe.vip
# FACTOR_LAB_LLM_MODEL=claude-opus-4-7
# FACTOR_LAB_LLM_API_KEY=sk-10a5b46...8491

# 启用 ai-continue
FACTOR_LAB_LLM_BASE_URL=https://rayplus.site
FACTOR_LAB_LLM_MODEL=gpt-5.4
FACTOR_LAB_LLM_API_KEY=sk-a49732a...ecb7
```

### 重启服务使配置生效
```bash
systemctl --user restart factor-lab-web-ui.service
```

---

## 📋 下一步计划

### 待实现的 WebUI 功能
根据之前的计划文档 `.hermes/plans/2026-04-24_043000-factor-lab-webui-control-panel-plan.md`：

1. **Settings 页面** - LLM 配置管理界面
   - 配置表单（Base URL, API Key, Model）
   - 测试连接功能
   - 保存配置到 .env

2. **Control 页面** - 守护进程控制界面
   - 启动/停止按钮
   - 状态显示
   - 日志查看

3. **安全增强**
   - API Key 脱敏显示
   - 配置文件权限检查
   - 本地访问限制

---

## 🎉 迁移总结

✅ **Factor Lab 已成功从 OpenClaw workspace 迁移到独立目录**

✅ **三个 LLM provider 配置已从 ccswitch/Hermes 导入**

✅ **系统可独立运行，不再依赖 OpenClaw**

✅ **WebUI 正常运行，可通过浏览器访问**

✅ **所有测试通过，系统状态正常**

---

## 📞 服务管理命令

```bash
# 查看服务状态
systemctl --user status factor-lab-web-ui.service

# 重启服务
systemctl --user restart factor-lab-web-ui.service

# 停止服务
systemctl --user stop factor-lab-web-ui.service

# 启动服务
systemctl --user start factor-lab-web-ui.service

# 查看日志
journalctl --user -u factor-lab-web-ui.service -f

# 禁用自启动
systemctl --user disable factor-lab-web-ui.service

# 启用自启动
systemctl --user enable factor-lab-web-ui.service
```

---

**迁移完成！系统已就绪。** 🚀
