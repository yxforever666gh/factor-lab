# 发布与 Git tag 同步

Factor Lab 用 annotated Git tag 标识正式版本。发布必须同时更新版本元数据、
`CHANGELOG.md`、GitHub `main` 和 GitHub tag；本地 tag 不算发布完成。

唯一支持的 tag 发布入口是：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

脚本固定使用 `origin`、`main` 和 `.github/workflows/ci.yml`。它只接受干净且与
`origin/main` 完全一致的提交，只接受该提交自己的 `main/push` CI 成功记录，并在推送
单个 annotated tag 后核对本地与 GitHub 的 tag object SHA 和 peeled commit。

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
   $buildRoot = "H:\Download\factor-lab-release-$tag"
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
4. 在全新的隔离 venv 中安装 wheel 与项目声明的精确依赖，验证包版本、`pip check` 和 CLI import；
   wheel 只包含 Python package，配置、协议与 evidence 仍由 checkout 提供，因此还要运行
   `python -m factor_lab.cli --root <git-archive-source-directory> strategy status`，证明安装包能读取并
   校验候选提交的完整研究闭包。
5. 用全新临时目录运行完整测试并编译生产源码：

   ```powershell
   $testRoot = "H:\Download\factor-lab-test-" + [guid]::NewGuid().ToString("N")
   python -m pytest tests -q --basetemp $testRoot -p no:cacheprovider
   python -m compileall -q src/factor_lab
   ```

6. 若 wheel 构建后又修改了 `src/factor_lab/**` 或 `pyproject.toml`，必须重新构建并重跑；
   仅 Changelog 的 wheel hash、测试或发布说明变化不改变包字节，但最终仍要确认 wheel 内全部
   `factor_lab/*.py` 与最终 release commit 的 Git blob 一致。
7. 提交最终发布元数据，确认工作树干净，推送 `main`，等待精确提交自己的双平台 CI 成功。

## 发布 tag

CI 绿后执行：

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
