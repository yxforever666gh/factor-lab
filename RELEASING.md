# 发布与 Git tag 同步

Factor Lab 用 annotated Git tag 标识正式版本。GitHub 只承担公开备份和 tag 同步；
构建、测试、安装与 CLI 验证全部在本机完成，GitHub Actions/CI/CD 保持关闭。发布必须同时
更新版本元数据、`CHANGELOG.md`、GitHub `main` 和 GitHub tag；本地 tag 不算发布完成。

唯一支持的 tag 发布入口是：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

脚本固定使用 `origin` 和 `main`，拒绝 GitHub Actions/workflow 依赖。它只接受干净且与
`origin/main` 完全一致、版本/Changelog/本机验证记录完整的提交，并在推送单个 annotated tag
后核对本地与 GitHub 的 tag object SHA 和 peeled commit。

## 版本规则

- 研究大方向改变：`major + 1`，minor 归零。
- 同一方向的小迭代：`minor + 1`。
- tag 使用 `major.minor`，Python 包使用等价的 `major.minor.0`。
- `pyproject.toml` 与 `src/factor_lab/__init__.py` 的版本必须一致。
- 已发布 tag 默认不可移动或删除；修正优先发布新版本。改写远端 tag 必须由用户明确确认，
  并在 Changelog 解释原因。

## 发布前

1. 把 `CHANGELOG.md` 的 `Unreleased` 内容移动到唯一的
   `## [<major.minor>] - YYYY-MM-DD` 段，并保留一个新的空 `Unreleased` 段。
2. 同步版本号，确认没有无关工作树改动。
3. 提交一个实现候选。构建物与临时环境放在 `H:\Download`，不要写入 Git：

   ```powershell
   $tag = "<major.minor>"
   $buildRoot = "H:\Download\FactorLabPytest\factor-lab-release-$tag"
   $archive = "$buildRoot\source.zip"
   $source = "$buildRoot\source"
   New-Item -ItemType Directory -Force -Path $source | Out-Null
   git archive --format=zip --output=$archive HEAD
   Expand-Archive -LiteralPath $archive -DestinationPath $source
   python -m pip wheel --no-deps --no-build-isolation `
     --wheel-dir "$buildRoot\wheelhouse" $source
   Get-FileHash -Algorithm SHA256 "$buildRoot\wheelhouse\*.whl"
   ```

   wheel 必须来自候选 commit 的 `git archive`，不能来自带未提交改动或 CRLF 转换的工作树。
   将 wheel SHA-256 写入对应 Changelog 版本段。
4. 在全新的隔离 venv 中安装 wheel 与项目声明的精确依赖，验证包版本、`pip check` 和 CLI import。
   wheel 只包含 Python package，配置、协议与 evidence 仍由 checkout 提供；因此用候选 commit 创建
   位于 `H:\Download` 的 fresh detached Git worktree，并运行
   `python -m factor_lab.cli --root <detached-worktree> strategy status`。不要对 `git archive` 解压目录运行
   该命令：6.1+ 完整性校验会读取 commit/tree/blob，缺少 `.git` 时必须失败。验证后通过
   `git worktree remove <detached-worktree>` 清理。
5. 用全新临时目录运行完整测试并编译生产源码；字节码也必须离开源码仓库：

   ```powershell
   $testRoot = "H:\Download\FactorLabPytest\factor-lab-test-" + [guid]::NewGuid().ToString("N")
   $env:PYTHONPYCACHEPREFIX = "H:\Download\FactorLabPytest\factor-lab-pycache-" + [guid]::NewGuid().ToString("N")
   python -m pytest tests -q --basetemp $testRoot -p no:cacheprovider
   python -m compileall -q src/factor_lab
   ```

6. 若 wheel 构建后又修改了 `src/factor_lab/**` 或 `pyproject.toml`，必须重新构建并重跑；
   仅 Changelog 的 wheel hash、测试或发布说明变化不改变包字节，但最终仍要确认 wheel 内全部
   `factor_lab/*.py` 与最终 release commit 的 Git blob 一致。
7. 将 wheel SHA、本机完整测试计数、编译、隔离安装、`pip check`、CLI status 与 wheel/source
   字节比较结果写入对应 Changelog 版本段。提交最终发布元数据，再确认工作树干净并推送 `main`；
   用 SSH `git ls-remote origin refs/heads/main` 核对远端 commit。不得用 GitHub Actions 替代本机验证。

## 发布 tag

本机验证完成、证据已写入 Changelog、最终 `main` 已通过 SSH 同步后执行：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

发布脚本成功输出中的 annotated object SHA 与 peeled commit 是远端完成证据。没有这两项核对，
不得声称 tag 已同步。若项目同时创建 GitHub Release，release notes 使用对应 Changelog 内容，
二进制作为 Release asset 上传并记录 SHA-256，不把大文件提交进 Git。

## 日常变更

用户可见的软件、数据合同、研究协议、修复、弃用和已知限制，都随改动写入
`CHANGELOG.md` 的 `Unreleased`。历史回测、十个相关 offset、测试通过或工程可运行都不能写成
未来盈利已验证；选择期和审计期必须说明实际是否独立。
