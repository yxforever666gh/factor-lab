# 发布与 Git tag 同步

本项目以 Git tag 标识正式版本。发布时必须同步更新版本元数据、`CHANGELOG.md`、本地
tag 和 GitHub 远端 tag；只在本地创建 tag 不算完成发布。

唯一支持的 tag 发布入口是 `scripts/publish-tag.ps1`。它只接受已与 GitHub `main`
完全一致、工作区干净且 GitHub CI 成功的提交，并在精确推送单个 tag 后校验远端对象
SHA 和目标 commit。不要用裸 `git tag` 代替它。

## 发布前

1. 确认工作区干净，并从准备发布的提交运行完整测试：

   ```powershell
   python -m pytest tests/unit tests/data tests/integration -q
   python -m compileall -q src/factor_lab
   ```

2. 将 `CHANGELOG.md` 的 `Unreleased` 条目移动到带日期的版本标题，并保留新的空
   `Unreleased` 段。
3. 同步更新 `pyproject.toml` 的 `project.version`。Git tag 使用 `<major>.<minor>`：研究
   大方向变化时 `major + 1` 并将 minor 归零，小方向迭代时 `minor + 1`；Python 包版本
   使用等价的 `<major>.<minor>.0`，并同步 `src/factor_lab/__init__.py`。
4. 提交发布变更，并记录 release commit SHA。

## 创建并同步 tag

数字发布 tag 使用 `<major>.<minor>`，并与 `pyproject.toml` 的 `<major>.<minor>.0`
对应。脚本默认创建 annotated tag，不移动已经发布的 tag：

```powershell
./scripts/publish-tag.ps1 -Tag <major.minor>
```

脚本内部执行的核心远端验证等价于：

```powershell
$localTagCommit = git rev-list -n 1 <tag>
$remoteTagLine = git ls-remote origin "refs/tags/<tag>^{}"
if (-not $remoteTagLine) {
    $remoteTagLine = git ls-remote origin "refs/tags/<tag>"
}
$remoteTagCommit = ($remoteTagLine -split "`t")[0]
if ($localTagCommit -ne $remoteTagCommit) {
    throw "Local and GitHub tag commits differ: $localTagCommit != $remoteTagCommit"
}
```

若项目使用 GitHub Release，再从已经同步的 tag 创建 Release，并使用对应版本的
Changelog 内容作为 release notes。没有验证远端 SHA 前，不应声明发布完成。

历史 `1.0`–`2.1` tag 不回写或改名；其中的 lightweight tag 是只读历史。
`research-os-final-YYYYMMDD` 是允许的归档 tag 命名例外。

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
