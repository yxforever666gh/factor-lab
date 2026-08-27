# 发布与 Git tag 同步

本项目以 Git tag 标识正式版本。发布时必须同步更新版本元数据、`CHANGELOG.md`、本地
tag 和 GitHub 远端 tag；只在本地创建 tag 不算完成发布。

唯一支持的正式版本 tag 发布入口是 `scripts/publish-tag.ps1 -Tag <major.minor>`。脚本只
接受 `-Tag`，并固定使用 `origin`、`main` 和 `.github/workflows/ci.yml`；调用者不能覆盖
这些发布边界。它只接受已与 `origin/main` 完全一致、工作区干净且精确 CI 运行成功的
提交，并在精确推送单个 tag 后校验远端 annotated tag 对象 SHA 和 peeled commit。不要
用裸 `git tag` 代替它。

## 发布前

1. 确认工作区干净，并从准备发布的提交运行完整测试：

   ```powershell
   python -m pytest tests/unit tests/data tests/integration -q
   python -m compileall -q src/factor_lab
   ```

2. 将 `CHANGELOG.md` 的 `Unreleased` 条目移动到唯一的
   `## [<major.minor>] - YYYY-MM-DD` 版本段，并保留恰好一个新的空 `## [Unreleased]`
   段。脚本会拒绝无效日期、重复版本标题、重复 `Unreleased` 或发布前仍含内容的
   `Unreleased`。
3. 同步更新 `pyproject.toml` 的 `project.version`。Git tag 使用 `<major>.<minor>`：研究
   大方向变化时 `major + 1` 并将 minor 归零，小方向迭代时 `minor + 1`；Python 包版本
   使用等价的 `<major>.<minor>.0`，并同步 `src/factor_lab/__init__.py`。
4. 提交发布变更，并记录 release commit SHA。

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
