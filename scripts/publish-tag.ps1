[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Tag,

    [ValidateNotNullOrEmpty()]
    [string]$Remote = "origin",

    [ValidateNotNullOrEmpty()]
    [string]$ReleaseBranch = "main",

    [ValidateNotNullOrEmpty()]
    [string]$Workflow = "ci.yml"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Git {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = @(& git @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Get-FirstField {
    param([AllowNull()][object]$Line)

    $text = [string]$Line
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }
    return ($text.Trim() -split "\s+")[0]
}

$branch = ([string](Invoke-Git @("branch", "--show-current") | Select-Object -First 1)).Trim()
if ($branch -ne $ReleaseBranch) {
    throw "Release tags must be published from '$ReleaseBranch'; current branch is '$branch'."
}

$dirty = @(Invoke-Git @("status", "--porcelain"))
if ($dirty.Count -gt 0) {
    throw "Working tree is not clean. Commit or remove changes before publishing a tag."
}

$head = ([string](Invoke-Git @("rev-parse", "HEAD") | Select-Object -First 1)).Trim()
$remoteMainLine = Invoke-Git @("ls-remote", "--heads", $Remote, "refs/heads/$ReleaseBranch") |
    Select-Object -First 1
$remoteMain = Get-FirstField $remoteMainLine
if (-not $remoteMain) {
    throw "Remote branch '$Remote/$ReleaseBranch' was not found."
}
if ($head -ne $remoteMain) {
    throw "HEAD ($head) is not the commit published at $Remote/$ReleaseBranch ($remoteMain)."
}

$isReleaseTag = $Tag -match '^\d+\.\d+$'
$isArchiveTag = $Tag -match '^research-os-final-\d{8}$'
if (-not ($isReleaseTag -or $isArchiveTag)) {
    throw "Tag must use major.minor (for example 3.0) or research-os-final-YYYYMMDD."
}

if ($isReleaseTag) {
    $pyproject = Get-Content -LiteralPath "pyproject.toml" -Raw
    $projectBlock = [regex]::Match($pyproject, '(?ms)^\[project\]\s*(.*?)(?=^\[|\z)')
    if (-not $projectBlock.Success) {
        throw "Could not find [project] in pyproject.toml."
    }
    $versionMatch = [regex]::Match($projectBlock.Groups[1].Value, '(?m)^version\s*=\s*"([^"]+)"\s*$')
    if (-not $versionMatch.Success) {
        throw "Could not find project.version in pyproject.toml."
    }
    $projectVersion = $versionMatch.Groups[1].Value
    $expectedProjectVersion = "$Tag.0"
    if ($expectedProjectVersion -ne $projectVersion) {
        throw "Release tag '$Tag' requires pyproject.toml version '$expectedProjectVersion', found '$projectVersion'."
    }
    $packageInit = Get-Content -LiteralPath "src/factor_lab/__init__.py" -Raw
    $packageVersionMatch = [regex]::Match(
        $packageInit,
        '(?m)^__version__\s*=\s*"([^"]+)"\s*$'
    )
    if (-not $packageVersionMatch.Success) {
        throw "Could not find __version__ in src/factor_lab/__init__.py."
    }
    $packageVersion = $packageVersionMatch.Groups[1].Value
    if ($packageVersion -ne $projectVersion) {
        throw "Package __version__ '$packageVersion' does not match pyproject.toml '$projectVersion'."
    }
}

$ghCommand = Get-Command gh -ErrorAction SilentlyContinue
if (-not $ghCommand) {
    throw "GitHub CLI ('gh') is required to verify CI before publishing."
}

$repo = (& gh repo view --json nameWithOwner --jq .nameWithOwner 2>&1)
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace([string]$repo)) {
    throw "Could not resolve the GitHub repository:`n$repo"
}
$repo = ([string]$repo).Trim()

$runJson = @(& gh run list --repo $repo --workflow $Workflow --commit $head --limit 20 `
    --json status,conclusion,headSha,databaseId,url 2>&1)
if ($LASTEXITCODE -ne 0) {
    throw "Could not read GitHub CI status:`n$($runJson -join [Environment]::NewLine)"
}
$runs = @((($runJson -join [Environment]::NewLine) | ConvertFrom-Json))
$latestRun = $runs | Where-Object { $_.headSha -eq $head } | Select-Object -First 1
if (-not $latestRun) {
    throw "No GitHub CI run was found for HEAD $head."
}
if ($latestRun.status -ne "completed" -or $latestRun.conclusion -ne "success") {
    throw "GitHub CI is not green for HEAD $head (status=$($latestRun.status), conclusion=$($latestRun.conclusion), url=$($latestRun.url))."
}

& git show-ref --verify --quiet "refs/tags/$Tag"
$localExists = $LASTEXITCODE -eq 0
$remoteTagLine = Invoke-Git @("ls-remote", "--refs", $Remote, "refs/tags/$Tag") |
    Select-Object -First 1
$remoteTagObject = Get-FirstField $remoteTagLine

if ($localExists) {
    $tagType = ([string](Invoke-Git @("cat-file", "-t", "refs/tags/$Tag") |
        Select-Object -First 1)).Trim()
    if ($tagType -ne "tag") {
        throw "Local tag '$Tag' is not annotated."
    }
    $tagCommit = ([string](Invoke-Git @("rev-list", "-n", "1", $Tag) |
        Select-Object -First 1)).Trim()
    if ($tagCommit -ne $head) {
        throw "Local tag '$Tag' points to $tagCommit instead of HEAD $head."
    }
} elseif ($remoteTagObject) {
    throw "Tag '$Tag' exists on GitHub but not locally. Fetch and inspect it instead of replacing it."
} else {
    Invoke-Git @("tag", "-a", $Tag, "-m", "Factor Lab $Tag") | Out-Null
}

$localTagObject = ([string](Invoke-Git @("rev-parse", "refs/tags/$Tag") |
    Select-Object -First 1)).Trim()

if ($remoteTagObject) {
    if ($localTagObject -ne $remoteTagObject) {
        throw "Local and GitHub tag objects differ: $localTagObject != $remoteTagObject."
    }
    Write-Host "Tag '$Tag' is already synchronized with GitHub at $localTagObject."
    exit 0
}

Invoke-Git @("push", $Remote, "refs/tags/${Tag}:refs/tags/${Tag}") | ForEach-Object {
    Write-Host $_
}

$verifiedRemoteLine = Invoke-Git @("ls-remote", "--refs", $Remote, "refs/tags/$Tag") |
    Select-Object -First 1
$verifiedRemoteObject = Get-FirstField $verifiedRemoteLine
if ($localTagObject -ne $verifiedRemoteObject) {
    throw "Remote verification failed: local=$localTagObject remote=$verifiedRemoteObject."
}

$localCommit = ([string](Invoke-Git @("rev-list", "-n", "1", $Tag) |
    Select-Object -First 1)).Trim()
$remotePeeledLine = Invoke-Git @("ls-remote", $Remote, "refs/tags/$Tag^{}") |
    Select-Object -First 1
$remoteCommit = Get-FirstField $remotePeeledLine
if ($localCommit -ne $remoteCommit) {
    throw "Remote tag target verification failed: local=$localCommit remote=$remoteCommit."
}

Write-Host "Published '$Tag' to GitHub and verified tag object $localTagObject at commit $localCommit."
