# 发布与 Git tag 同步

本项目以 Git tag 标识正式版本。发布时必须同步更新版本元数据、`CHANGELOG.md`、本地
tag 和 GitHub 远端 tag；只在本地创建 tag 不算完成发布。

唯一支持的正式版本 tag 发布入口是 `scripts/publish-tag.ps1 -Tag <major.minor>`。脚本只
接受 `-Tag`，并固定使用 `origin`、`main` 和 `.github/workflows/ci.yml`；调用者不能覆盖
这些发布边界。它只接受已与 `origin/main` 完全一致、工作区干净且精确 CI 运行成功的
提交，并在精确推送单个 tag 后校验远端 annotated tag 对象 SHA 和 peeled commit。不要
用裸 `git tag` 代替它。

## 发布前

1. 将 `CHANGELOG.md` 的 `Unreleased` 条目移动到唯一的
   `## [<major.minor>] - YYYY-MM-DD` 版本段，并保留恰好一个新的空 `## [Unreleased]`
   段。脚本会拒绝无效日期、重复版本标题、重复 `Unreleased` 或发布前仍含内容的
   `Unreleased`。
2. 同步更新 `pyproject.toml` 的 `project.version`。Git tag 使用 `<major>.<minor>`：研究
   大方向变化时 `major + 1` 并将 minor 归零，小方向迭代时 `minor + 1`；Python 包版本
   使用等价的 `<major>.<minor>.0`，并同步 `src/factor_lab/__init__.py`。前瞻实现发布还必须
   同步冻结 manifest 的 `implementation_release`、CLI `prospective upgrade` 默认 tag 与
   对应测试；协议身份不随纠错实现版本机械改名。
3. 数字发布和前瞻运行只使用项目内的版本专用环境
   `runtime/environments/<implementation-release>`。必须用 manifest 将要绑定的精确 CPython
   build 创建
   该环境，先把全部精确第三方 artifact 保存到该环境的 `wheelhouse` 并准备构建环境；源码
   最终确定后以 `--no-build-isolation --no-deps` 构建一次当前项目 wheel，将其 SHA-256 连同
   全部第三方 artifact 写入 `protocols/<protocol-version>-runtime-lock.txt`。正式环境最终只从这份
   完整 lock 安装，不保留 editable metadata。不得复用系统/user site-packages，也不得让
   pip 在最终安装阶段临时联网解析。当前 5.x 纠错实现继续使用冻结的
   `protocols/5.2-target-generator.json` 与 `protocols/5.2-runtime-lock.txt`：

   ```powershell
   $releaseEnv = "runtime/environments/<implementation-release>"
   $releasePython = (Resolve-Path `
     "$releaseEnv/Scripts/python.exe").Path
   $releaseWheelhouse = (Resolve-Path "$releaseEnv/wheelhouse").Path

   # 源码最终确定后先提交一个 clean implementation candidate；从该 commit 的
   # `git archive`（不是带 CRLF/未提交改动的工作树）展开临时源码，再用已锁定的
   # setuptools/wheel 构建项目 wheel。计算 SHA-256，将
   # factor-research-mvp==<version> 及该 hash 加入 runtime lock 后再做最终安装。
   & $releasePython -m pip wheel --no-deps --no-build-isolation `
     --wheel-dir $releaseWheelhouse <git-archive-source-directory>
   & $releasePython -m pip install --force-reinstall --no-index `
     --find-links $releaseWheelhouse --require-hashes `
     -r "protocols/<protocol-version>-runtime-lock.txt"

   @'
   from pathlib import Path
   import importlib.metadata as metadata
   import factor_lab

   declared = next(
       line.split("=", 1)[1].strip().strip(chr(34))
       for line in Path("pyproject.toml").read_text(encoding="utf-8").splitlines()
       if line.startswith("version = ")
   )
   installed = metadata.version("factor-research-mvp")
   assert declared == factor_lab.__version__ == installed, (
       declared,
       factor_lab.__version__,
       installed,
   )
   print(f"release version: {installed}")
   '@ | & $releasePython -
   ```

   必须另建一个全新的项目内 smoke venv，重复上述离线 `--require-hashes` 安装并成功 import，
   证明 wheelhouse 不是仅因当前环境残留而可用；逐字节比较 self-wheel 内所有
   `factor_lab/*.py` 与最终 release commit 的 Git blob，且最终 commit 不得再修改 wheel
   所含源码或 `pyproject.toml`。发布后保留版本 wheelhouse；
   若同时发布 GitHub Release，则上传其归档并记录 SHA-256，不把大二进制写入 Git 历史。

4. 确认除本次发布准备外没有无关改动，并运行完整测试。每次都生成全新的
   `runtime/test-tmp/<unique>`，不得复用旧目录；同时关闭 pytest cache provider，避免 Windows
   ACL 或历史缓存污染发布证据：

   ```powershell
   $releaseTestRun = "release-" + [guid]::NewGuid().ToString("N")
   & $releasePython -m pytest tests/unit tests/data tests/integration -q `
     --basetemp "runtime/test-tmp/$releaseTestRun" -p no:cacheprovider
   & $releasePython -m compileall -q src/factor_lab
   ```

5. 若版本包含前瞻运行实现，在源文件、依赖 pin 和版本号都最终确定后刷新运行闭包，再重跑
   完整测试；闭包会绑定全部 `src/factor_lab/**/*.py`、`configs/data.json`、`pyproject.toml`
   与实际数值/数据依赖版本：

   ```powershell
   & $releasePython scripts/update-runtime-closure.py `
     --manifest protocols/<protocol-version>-target-generator.json

   $closureTestRun = "release-closure-" + [guid]::NewGuid().ToString("N")
   & $releasePython -m pytest tests/unit tests/data tests/integration -q `
     --basetemp "runtime/test-tmp/$closureTestRun" -p no:cacheprovider
   & $releasePython -m compileall -q src/factor_lab
   ```

   刷新后不得再修改闭包内文件；如有修改必须再次刷新并重跑测试。manifest 自身不放进自引用
   的 file list，而由发布 tag、manifest SHA 和 release capsule 单独逐字节绑定。updater 还会
   拒绝已安装 `factor-research-mvp` 与 manifest `implementation_release` 不一致的环境。
6. 提交发布变更，并记录 release commit SHA。正式发布前确认工作区干净，并确认本地 `main`
   与 `origin/main` 完全一致；发布脚本会再次强制核验这两项。

若发布将启用前瞻实现，implementation canary 的可信 transparency-log 时间决定第一条不可
跳过的官方 signal：canary Tlog 之后的首个官方收盘必须入账。发布 runbook 必须事先写明目标
首信号及 canary 窗口；5.2 要保持 2026-08-31 为首信号，canary Tlog 必须位于
2026-08-28 15:00（不含）至 2026-08-31 15:00（不含），时区均为 Asia/Shanghai。
在首个 decision 前发布的 5.3、5.4、5.5 等纠错实现仍受同一首信号窗口约束。
首次 implementation canary 的可信 TLog 是不可变 prospective epoch；后续 canary 不能以较晚
TLog 重基准首信号。若 active 纠错 canary 不早于该固定 signal 收盘，控制器必须 terminal，不能
把该 signal 记为 skipped 或推进到下一交易日。

跨版本 implementation transition 只允许尚无任何 sealed decision 的账本。历史 capsule 必须
静态验证 annotated tag、Git blobs、closure、receipt 与完整 capsule tree，最终 active capsule
仍必须逐值匹配当前正式环境。若新升级记录已追加后又被 abandonment，单调历史不会回退；恢复
运行必须发布并追加更高的纠正版，而不是移动旧 tag 或假装旧解释器能越过中间记录。

## 创建并同步 tag

数字发布 tag 只接受 canonical `<major>.<minor>`：两个分量均为十进制非负整数，除单独
的 `0` 外不得有前导零；它必须与 `pyproject.toml` 和
`src/factor_lab/__init__.py` 中完全一致的 `<major>.<minor>.0` 对应。脚本创建 annotated
tag，但绝不移动或删除已有 tag：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

脚本只接受以下精确 CI 证据：workflow 文件 `ci.yml`、分支 `main`、事件 `push`、
`headSha` 等于当前 release commit，且 `status=completed`、`conclusion=success`。PR、
手动触发、其他分支、其他 workflow 或其他 commit 的成功运行都不能解锁发布。

无论 tag 是本次新推送，还是此前已同步，脚本都会分别验证 annotated tag 对象与 peeled
commit；已有 tag 不会因为“已经存在”而跳过目标校验。核心远端验证等价于：

```powershell
$localTagCommit = git rev-list -n 1 <tag>
$localTagObject = git rev-parse "refs/tags/<tag>"
$remoteTagObject = ((git ls-remote origin "refs/tags/<tag>") -split "`t")[0]
$remoteTagCommit = ((git ls-remote origin "refs/tags/<tag>^{}") -split "`t")[0]
if ($localTagObject -ne $remoteTagObject -or $localTagCommit -ne $remoteTagCommit) {
    throw "Local and GitHub annotated tag evidence differs"
}
```

若项目使用 GitHub Release，再从已经同步的 tag 创建 Release，并使用对应版本的
Changelog 内容作为 release notes。没有验证远端 SHA 前，不应声明发布完成。

历史 `1.0`–`2.1` tag 不回写或改名；其中的 lightweight tag 是只读历史。已有
`research-os-final-YYYYMMDD` 归档 tag 同样只读；正式发布脚本不再接受或创建归档 tag。

## 修改或撤销 tag

- 已经推送到 GitHub 的正式 tag 默认不可移动；修正应发布新版本。
- 删除或强制移动远端 tag 会改写公开发布历史，必须先获得明确确认，并在 Changelog
  说明原因。
- 归档 tag（例如 `research-os-final-20260826`）同样不可静默覆盖。

## 每次普通变更

- 对用户可见的软件、数据合同、研究协议、修复、已知限制和弃用项，随提交更新
  `CHANGELOG.md` 的 `Unreleased` 段。
- 不把回测收益、工程 gate 或运行状态写成“验证成功”，除非证据确实满足项目当时的
  独立验证协议。
