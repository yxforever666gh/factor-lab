[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Tag
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Remote = "origin"
$ReleaseBranch = "main"
$Workflow = "ci.yml"

function Invoke-Git {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = @(& git @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Invoke-Gh {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = @(& gh @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "gh $($Arguments -join ' ') failed:`n$($output -join [Environment]::NewLine)"
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

function Get-RemoteRefSha {
    param([Parameter(Mandatory = $true)][string]$Ref)

    $line = Invoke-Git @("ls-remote", $Remote, $Ref) | Select-Object -First 1
    return Get-FirstField $line
}

function Assert-ReleaseCommitState {
    $branch = ([string](Invoke-Git @("branch", "--show-current") | Select-Object -First 1)).Trim()
    if ($branch -ne $ReleaseBranch) {
        throw "Release tags must be published from '$ReleaseBranch'; current branch is '$branch'."
    }

    $dirty = @(Invoke-Git @("status", "--porcelain"))
    if ($dirty.Count -gt 0) {
        throw "Working tree is not clean. Commit or remove changes before publishing a tag."
    }

    $currentHead = ([string](Invoke-Git @("rev-parse", "HEAD") | Select-Object -First 1)).Trim()
    $remoteMain = Get-RemoteRefSha "refs/heads/$ReleaseBranch"
    if (-not $remoteMain) {
        throw "Remote branch '$Remote/$ReleaseBranch' was not found."
    }
    if ($currentHead -ne $remoteMain) {
        throw "HEAD ($currentHead) is not the commit published at $Remote/$ReleaseBranch ($remoteMain)."
    }
    return $currentHead
}

if ($Tag -notmatch '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$') {
    throw "Tag must use canonical major.minor digits without leading zeroes (for example 4.1)."
}

$head = Assert-ReleaseCommitState

$pyproject = Get-Content -LiteralPath "pyproject.toml" -Raw
$projectBlocks = [regex]::Matches(
    $pyproject,
    '(?ms)^\[project\][ \t]*\r?$\n(?<body>.*?)(?=^\[|\z)'
)
if ($projectBlocks.Count -ne 1) {
    throw "Expected exactly one [project] table in pyproject.toml."
}
$versionMatches = [regex]::Matches(
    $projectBlocks[0].Groups["body"].Value,
    '(?m)^version[ \t]*=[ \t]*["'']([^"'']+)["''][ \t]*\r?$'
)
if ($versionMatches.Count -ne 1) {
    throw "Expected exactly one project.version in pyproject.toml."
}
$projectVersion = $versionMatches[0].Groups[1].Value
$expectedProjectVersion = "${Tag}.0"
if ($projectVersion -ne $expectedProjectVersion) {
    throw "Release tag '$Tag' requires pyproject.toml version '$expectedProjectVersion', found '$projectVersion'."
}

$packageInit = Get-Content -LiteralPath "src/factor_lab/__init__.py" -Raw
$packageVersionMatches = [regex]::Matches(
    $packageInit,
    '(?m)^__version__[ \t]*=[ \t]*["'']([^"'']+)["''][ \t]*\r?$'
)
if ($packageVersionMatches.Count -ne 1) {
    throw "Expected exactly one __version__ in src/factor_lab/__init__.py."
}
$packageVersion = $packageVersionMatches[0].Groups[1].Value
if ($packageVersion -ne $expectedProjectVersion) {
    throw "Release tag '$Tag' requires package __version__ '$expectedProjectVersion', found '$packageVersion'."
}

$changelog = Get-Content -LiteralPath "CHANGELOG.md" -Raw
$unreleasedHeadings = [regex]::Matches(
    $changelog,
    '(?m)^## \[Unreleased\][ \t]*\r?$'
)
if ($unreleasedHeadings.Count -ne 1) {
    throw "CHANGELOG.md must contain exactly one '## [Unreleased]' section."
}
$unreleasedSections = [regex]::Matches(
    $changelog,
    '(?ms)^## \[Unreleased\][ \t]*\r?\n(?<body>.*?)(?=^## \[|\z)'
)
if ($unreleasedSections.Count -ne 1) {
    throw "Could not parse the unique CHANGELOG.md Unreleased section."
}
if (-not [string]::IsNullOrWhiteSpace($unreleasedSections[0].Groups["body"].Value)) {
    throw "CHANGELOG.md Unreleased must be empty before publishing '$Tag'."
}

$escapedTag = [regex]::Escape($Tag)
$tagHeadings = [regex]::Matches(
    $changelog,
    "(?m)^## \[$escapedTag\][^`r`n]*`r?$"
)
$datedTagHeadings = [regex]::Matches(
    $changelog,
    "(?m)^## \[$escapedTag\] - (?<date>[0-9]{4}-[0-9]{2}-[0-9]{2})[ `t]*`r?$"
)
if ($tagHeadings.Count -ne 1 -or $datedTagHeadings.Count -ne 1) {
    throw "CHANGELOG.md must contain exactly one '## [$Tag] - YYYY-MM-DD' release section."
}
$releaseDateText = $datedTagHeadings[0].Groups["date"].Value
$releaseDate = [datetime]::MinValue
$validReleaseDate = [datetime]::TryParseExact(
    $releaseDateText,
    "yyyy-MM-dd",
    [Globalization.CultureInfo]::InvariantCulture,
    [Globalization.DateTimeStyles]::None,
    [ref]$releaseDate
)
if (-not $validReleaseDate -or $releaseDate.ToString("yyyy-MM-dd") -ne $releaseDateText) {
    throw "CHANGELOG.md release date '$releaseDateText' is not a valid ISO date."
}

$ghCommand = Get-Command gh -ErrorAction SilentlyContinue
if (-not $ghCommand) {
    throw "GitHub CLI ('gh') is required to verify CI before publishing."
}

$repoJson = ((Invoke-Gh @("repo", "view", "--json", "nameWithOwner")) -join [Environment]::NewLine)
try {
    $repoObject = $repoJson | ConvertFrom-Json
} catch {
    throw "Could not parse the GitHub repository response: $repoJson"
}
$repo = [string]$repoObject.nameWithOwner
if ([string]::IsNullOrWhiteSpace($repo)) {
    throw "Could not resolve the GitHub repository."
}

$runJson = ((Invoke-Gh @(
    "run", "list",
    "--repo", $repo.Trim(),
    "--workflow", $Workflow,
    "--branch", $ReleaseBranch,
    "--event", "push",
    "--commit", $head,
    "--limit", "20",
    "--json", "status,conclusion,headSha,headBranch,event,databaseId,url"
)) -join [Environment]::NewLine)
try {
    $runs = @($runJson | ConvertFrom-Json)
} catch {
    throw "Could not parse GitHub CI status: $runJson"
}
$latestRun = $runs |
    Where-Object {
        $_.headSha -eq $head -and
        $_.headBranch -eq $ReleaseBranch -and
        $_.event -eq "push"
    } |
    Select-Object -First 1
if (-not $latestRun) {
    throw "No '$Workflow' push run on '$ReleaseBranch' was found for HEAD $head."
}
if ($latestRun.status -ne "completed" -or $latestRun.conclusion -ne "success") {
    throw "GitHub CI '$Workflow' is not green for HEAD $head (status=$($latestRun.status), conclusion=$($latestRun.conclusion), url=$($latestRun.url))."
}

# Re-check the immutable release boundary after the network CI lookup.
$verifiedHead = Assert-ReleaseCommitState
if ($verifiedHead -ne $head) {
    throw "HEAD changed during release validation: $head -> $verifiedHead."
}

& git show-ref --verify --quiet "refs/tags/$Tag"
$localExists = $LASTEXITCODE -eq 0
$remoteTagObject = Get-RemoteRefSha "refs/tags/$Tag"
$remoteWasPresent = -not [string]::IsNullOrWhiteSpace([string]$remoteTagObject)

if ($localExists) {
    $tagType = ([string](Invoke-Git @("cat-file", "-t", "refs/tags/$Tag") |
        Select-Object -First 1)).Trim()
    if ($tagType -ne "tag") {
        throw "Local tag '$Tag' is not annotated."
    }
    $tagCommit = ([string](Invoke-Git @("rev-parse", "refs/tags/$Tag^{}") |
        Select-Object -First 1)).Trim()
    if ($tagCommit -ne $head) {
        throw "Local tag '$Tag' points to $tagCommit instead of HEAD $head."
    }
} elseif ($remoteWasPresent) {
    throw "Tag '$Tag' exists on GitHub but not locally. Fetch and inspect it instead of replacing it."
} else {
    Invoke-Git @("tag", "-a", $Tag, "-m", "Factor Lab $Tag") | Out-Null
}

$localTagType = ([string](Invoke-Git @("cat-file", "-t", "refs/tags/$Tag") |
    Select-Object -First 1)).Trim()
if ($localTagType -ne "tag") {
    throw "Local tag '$Tag' is not annotated."
}
$localTagObject = ([string](Invoke-Git @("rev-parse", "refs/tags/$Tag") |
    Select-Object -First 1)).Trim()
$localCommit = ([string](Invoke-Git @("rev-parse", "refs/tags/$Tag^{}") |
    Select-Object -First 1)).Trim()
if ($localCommit -ne $head) {
    throw "Local tag '$Tag' points to $localCommit instead of HEAD $head."
}

if ($remoteWasPresent) {
    if ($localTagObject -ne $remoteTagObject) {
        throw "Local and GitHub tag objects differ: $localTagObject != $remoteTagObject."
    }
} else {
    Invoke-Git @("push", $Remote, "refs/tags/${Tag}:refs/tags/${Tag}") | ForEach-Object {
        Write-Host $_
    }
}

# This verification is mandatory for both a newly pushed and a pre-existing tag.
$verifiedRemoteObject = Get-RemoteRefSha "refs/tags/$Tag"
if ($localTagObject -ne $verifiedRemoteObject) {
    throw "Remote tag object verification failed: local=$localTagObject remote=$verifiedRemoteObject."
}
$verifiedRemoteCommit = Get-RemoteRefSha "refs/tags/$Tag^{}"
if ($localCommit -ne $verifiedRemoteCommit) {
    throw "Remote peeled target verification failed: local=$localCommit remote=$verifiedRemoteCommit."
}

if ($remoteWasPresent) {
    Write-Host "Tag '$Tag' was already synchronized; verified annotated object $localTagObject and peeled commit $localCommit on GitHub."
} else {
    Write-Host "Published '$Tag' to GitHub and verified annotated object $localTagObject and peeled commit $localCommit."
}
