# Session: 2026-03-20 00:29:34 UTC

- **Session Key**: agent:main:main
- **Session ID**: afd5648b-69bb-4487-aaf2-2396f3649b58
- **Source**: webchat

## Conversation Summary

user: System: [2026-03-20 08:14:39 GMT+8] Exec completed (vivid-sh, code 0) :: 9 CST; 7ms ago Main PID: 1073460 (python3) Tasks: 1 (limit: 8612) Memory: 1.4M CPU: 4ms CGroup: /user.slice/user-1000.slice/user@1000.service/app.slice/factor-lab-research-daemon.…
System: [2026-03-20 08:15:10 GMT+8] Exec completed (oceanic-, code 0) :: CST; 30ms ago Main PID: 1073652 (python3) Tasks: 1 (limit: 8612) Memory: 3.4M CPU: 24ms CGroup: /user.slice/user-1000.slice/user@1000.service/app.slice/factor-lab-research-daemon.…
System: [2026-03-20 08:17:04 GMT+8] Exec completed (lucky-em, code 0) :: ge_clean_ms": 31.993, "feature_build_ms": 31.993, "cache_write_ms": 0.556, "prepare_window_45d_ms": 109511.288, "prepare_window_90d_ms": 17.665, "cache_hit_type": "request_exact",…

Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Fri 2026-03-20 08:20 GMT+8] 现在的主要耗时在哪个部分？
assistant: [[reply_to_current]] 现在看，**主要耗时还是在数据获取，而且几乎集中在第一次铺缓存时的 Tushare 拉取**。

### 从刚才真实预热结果看
这轮预热里大头非常明显：

- `stock_basic_ms`：**约 9.1s**
- `daily_fetch_ms`：**约 77.3s**
- `daily_basic_fetch_ms`：同一段统计里一起记了，核心也是这块
- `merge_clean_ms`：**几十毫秒**
- `feature_build_ms`：**几十毫秒**
- 后续窗口：
  - `90d`：**31.9ms**
  - `120d`：**34.2ms**
  - `180d`：**41.7ms**
  - `240d`：**51.9ms**
  - `480d`：**96.9ms**

### 所以结论很直接
## 当前最大耗时 = **首次数据拉取**
更具体点：

### 1. 最大头：`daily` / `daily_basic` API 拉取
也就是：
- 按股票
- 按时间块
- 向 Tushare 拉历史行情和基本面

这是现在绝对第一大头。

### 2. 次大头：`stock_basic`
取股票基础列表这一步也不便宜，差不多 **9 秒**

### 3. 本地处理其实已经不算慢
包括：
- merge / clean
- feature build
- cache write
- workflow 后续分析

这些现在和取数比起来，已经算小头了。

---

# 一句话
**现在主要耗时不在分析，而在第一次打 Tushare 把底层数据拉下来。**

缓存一旦铺好后：
- 后续窗口已经接近毫秒级/秒级
- 说明本地切片和分析链路不是主要瓶颈

---

# 所以接下来如果还想继续提速，重点就不是 workflow 主体了，而是继续压这几个点：

## 优先级 1：减少 Tushare 请求次数
这是最关键的：
- 更粗的时间块
- 更少的碎请求
- 更少重复请求

## 优先级 2：减少 `stock_basic` 重复拉取
- 这玩意现在也偏慢
- 可以单独缓存更久

## 优先级 3：继续让预热更聪明
- 让 daemon 只补新增日期
- 不要重复碰老区间

---

# 换句话说
现在系统已经从：

> “每次 workflow 都慢”

变成了：

> “第一次建缓存慢，后面大多已经快了”

这其实是很大的进展。

如果你要，我下一步可以继续给你拆：
**当前 77 秒的 `daily fetch` 里面，到底是“请求太多”还是“单次请求太慢”更占主因。**  
这个能直接决定下一刀该砍哪里。
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Fri 2026-03-20 08:21 GMT+8] tushare绕开tun代理会不会快一点？
assistant: [[reply_to_current]] **有可能，会快。**  
尤其如果你现在的 tun 代理带来的是：

- 更高延迟
- 更差的 TCP 稳定性
- 更慢的 DNS
- 或者跨区绕路

那 Tushare 这种**大量小到中等 HTTP 请求**的场景，通常会被放大得很明显。

### 为什么可能会快
你现在最慢的是：
- `stock_basic`
- `daily`
- `daily_basic`

这类请求的特点不是超大带宽下载，而是：

- 请求很多
- 往返多
- 对延迟敏感
- 对连接稳定性敏感

所以如果 tun 代理让每个请求多出一点延迟，累计起来就会很夸张。  
比如每次请求多 300ms，几十次请求就是十几秒起步。

---

## 什么时候“绕开 tun”最可能有收益
如果你现在的网络是这种情况，收益会比较明显：

- tun 出口在海外，Tushare 服务在国内链路更优
- tun 路由不稳定，偶尔抖动/重传
- DNS 走代理导致解析慢
- 代理对 HTTPS 握手和连接复用不友好

这种场景下，**直连国内网络**通常会明显更顺。

---

## 但我不会直接拍胸说“一定”
因为还有另一种可能：

- 你的直连本身就更差
- tun 反而做了更优的出口路由
- 或者当前机器对 Tushare 的直连被运营商路径绕远

所以这事最靠谱的办法不是猜，而是：

## 直接做 A/B 测试
拿同一个预热命令，分别测：

### A. 走当前 tun
记录：
- 总耗时
- `stock_basic_ms`
- `daily_fetch_ms`
- `api_call_count`

### B. 绕开 tun
再记录同样指标

只要测一轮，你就知道值不值得切。

---

## 我建议你怎么测
就测这个最有代表性的：

- `prepare_tushare_data.py --window-days 45 --window-days 90`

或者直接测一次较完整预热：
- `45,90,120,180,240,480`

然后对比：
- 总耗时
- `stock_basic_ms`
- `daily_fetch_ms`

### 判断标准
如果绕开 tun 后：
- `daily_fetch_ms` 明显下降 20%+  
那就值得继续做路由优化。

---

## 如果真要绕开，最佳方式不是“全局关代理”
而是：

### 方案 1：只让 Tushare 直连
最理想：
- 仅对 Tushare 域名/IP 走直连
- 其他流量继续走 tun

这样最稳，也不影响别的组件。

### 方案 2：给 research daemon 单独网络策略
让：
- daemon / python / Tushare 请求
- 走直连或特定路由表

这也比整机乱改强。

---

## 我的判断
**从你现在这个耗时结构看，绕开 tun 是值得试的。**  
因为你当前瓶颈很像典型的“高请求次数 + 高 RTT 成本”问题，网络路径优化大概率有感知收益。

如果你要，我下一步可以继续两种方式之一：

1. **我先帮你设计一个最小 A/B benchmark 方案**，你直接跑  
2. **我直接在这台机子上帮你测当前 Tushare 请求链路**，先看 DNS / RTT / 出口路径有没有明显问题
user: Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Fri 2026-03-20 08:21 GMT+8] 测试一下
assistant: 我先做个最小 A/B：看当前代理环境、路由，再直接测 Tushare 一个轻量接口的往返时间。
assistant: 我已经确认到：**Tushare 现在确实走 `Mihomo` tun**，不是直连。接着我直接测一下同一目标走默认路由 vs 强制 `eth1` 的差异。
assistant: [[reply_to_current]] 测出来了，**绕开 tun 大概率会明显更快**，而且差距不小。

### 我确认到的事实
Tushare 目标现在确实走的是 `Mihomo` tun：

- `api.waditu.com -> 60.205.198.20 / 8.140.225.26`
- 路由结果：
  - `via 198.18.0.2 dev Mihomo table 2022`

也就是说，**当前 Tushare 请求是被策略路由进 tun 的**。

---

## A/B 测试结果
我对同一个目标 `http://api.waditu.com/dataapi/stock_basic` 做了两组测试：

### 1. 默认路由（当前，走 tun）
- run1: **0.648s**
- run2: **0.733s**
- run3: **1.568s**

### 2. 强制 `eth1`（绕开 tun）
- run1: **0.063s**
- run2: **0.078s**
- run3: **0.078s**

---

## 结论
### 走直连 `eth1` 比走 tun 快很多
粗看大概是：

- **快 8 倍到 20 倍左右**
- 而且直连的抖动明显更小、更稳

这和你现在的耗时结构完全对得上。  
因为 Tushare 不是一两个超大请求，而是**很多请求累积**，所以这种单次 RTT / 首包差异会被放大成几十秒。

---

## 这意味着什么
你现在的大头 `daily_fetch_ms ~ 77s`，有相当大概率就是被 tun 放大了。  
不能保证线性缩短到同样比例，但：

> **给 Tushare 绕开 tun，是非常值得做的下一刀。**

---

## 顺手补一个参考
我还测了真实 `tushare stock_basic` 调用（当前默认路径）：

- 第 1 次：**4.859s**
- 第 2 次：**2.304s**
- 第 3 次：**1.859s**

这和上面的 HTTP A/B 结果也一致：  
**当前默认路径就是偏慢，而且有波动。**

---

## 我建议下一步
最合理的做法不是全局关代理，而是：

### 方案 A：只给 Tushare 做直连例外
让这些目标绕开 tun：
- `api.waditu.com`
- 以及它解析到的目标 IP / 对应域名策略

### 方案 B：让 research daemon / Python 进程单独走直连
如果你更想从进程级切，而不是改代理规则，也可以。

---

## 一句话结论
**是的，Tushare 绕开 tun 代理会更快，而且从实测看，不是小优化，是明显优化。**

如果你点头，我下一步就直接帮你做：
1. **给 Tushare 加直连规则**
2. **再复测一次预热耗时**
3. **告诉你实际省了多少秒**
user: System: [2026-03-20 08:22:17 GMT+8] Exec completed (quiet-va, code 0) :: run=1 rows=5490 seconds=4.859 run=2 rows=5490 seconds=2.304 run=3 rows=5490 seconds=1.859

Sender (untrusted metadata):
```json
{
  "label": "openclaw-control-ui",
  "id": "openclaw-control-ui"
}
```

[Fri 2026-03-20 08:25 GMT+8] 给 Tushare 加直连规则
assistant: 我先找当前 Mihomo/OpenClaw 的实际配置入口，尽量只给 Tushare 做直连，不碰别的流量。
assistant: 我加到 Mihomo 的全局 override 里，优先命中 `waditu.com` 相关域名走 `DIRECT`。然后尽量触发一次代理核心重载。
assistant: 我把生效中的 `work/config.yaml` 也补上了。现在差最后一步：让 Mihomo sidecar 重载配置，我先试最小动作，不行再告诉你。
assistant: 我找到一个关键点：**Mihomo 现在是 `mode: global`**，在这个模式下，域名规则基本不生效，所以刚才那条 `waditu.com -> DIRECT` 规则没有把速度拉下来。  
要真正绕开，我准备直接给 **Tushare IP 加系统级直连路由规则**。
assistant: 我已经把**当前会话的 Tushare 直连路由**打上了，路由现在明确从 `eth1` 直出。  
我顺手把它做成一个小的 systemd user 服务，免得下次重启丢掉。
assistant: [[reply_to_current]] 搞定了，**Tushare 现在已经加上直连规则**，而且我也做了持久化。

### 我做了什么
1. **实时生效**
   - 给这几个 Tushare 目标 IP 加了系统级直连路由：
     - `60.205.198.20`
     - `8.140.225.26`
     - `47.94.110.54`
   - 现在它们明确走 `eth1` / `main` 路由，不再进 `Mihomo` tun

2. **持久化**
   - 新增脚本：
     - `scripts/apply_tushare_direct_routes.sh`
   - 新增并启用了 user service：
     - `~/.config/systemd/user/tushare-direct-routes.service`

3. **顺手确认了原因**
   - Mihomo 当前是 `mode: global`
   - 所以单纯加域名 `DIRECT` 规则并不够
   - 这次改成了更直接有效的**系统路由级绕过**

### 实测结果
#### HTTP 直连效果
改完后同一目标的请求时间变成：

- `0.065s`
- `0.117s`
- `0.065s`

之前默认走 tun 时大概是：

- `0.648s`
- `0.733s`
- `1.568s`

#### 真实 Tushare `stock_basic` 调用
改完后：

- `0.200s`
- `0.244s`
- `0.200s`

改之前是：

- `4.859s`
- `2.304s`
- `1.859s`

### 结论
这个优化是**非常值的**。  
对 `stock_basic` 这种请求，实测已经从 **2~5 秒级** 掉到 **0.2 秒级**，差不多是 **一个数量级的提升**。

### 提交记录
- `853b877` — `add direct routing support for tushare`

另外，**生效中的 user service 配置**我也已经在系统里启用了，所以下次会继续自动打这几条直连路由。

如果你要，我下一步就继续：
1. **重新跑一轮完整预热 benchmark**
2. 直接告诉你现在 `daily_fetch_ms / total_ms` 还剩多少，看看整体省了多少秒
