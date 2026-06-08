# SEC Derivatives Scheduler — local cron replacement for GitHub Actions
# Runs as a Windows Scheduled Task, weekdays at 06:00 UTC

$ErrorActionPreference = "Stop"
$RepoDir = "C:\Users\lukasbecker\claudeprojects\sec-derivatives"
$LogDir  = "$RepoDir\logs"
$LogFile = "$LogDir\scheduler_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').log"

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }

function Log { param([string]$Msg)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $Msg" | Tee-Object -FilePath $LogFile -Append
}

try {
    Log "=== Scheduler run started ==="

    Set-Location $RepoDir

    # Pull latest
    Log "Pulling latest from origin/master..."
    git pull origin master 2>&1 | Out-String | ForEach-Object { Log $_ }

    # Install/upgrade dependencies
    Log "Installing dependencies..."
    python -m pip install -q -r requirements.txt 2>&1 | Out-String | ForEach-Object { Log $_ }

    # Run the scheduler
    Log "Running scheduler..."
    python -m src.scheduler --max-activations 50 --since 2025-01-01 --json-summary summary.json --verbose 2>&1 | Out-String | ForEach-Object { Log $_ }
    $ExitCode = $LASTEXITCODE

    if ($ExitCode -ne 0) {
        Log "ERROR: Scheduler exited with code $ExitCode"
    } else {
        Log "Scheduler completed successfully."
    }

    # Commit and push changes
    Log "Staging changes..."
    git add registry/ profiles/ filer_profiles/ output/

    $HasChanges = git diff --staged --quiet 2>&1; $changed = $LASTEXITCODE
    if ($changed -ne 0) {
        # Build commit message from summary.json
        $CommitMsg = "chore(scheduler): scheduled run"
        try {
            $summary = Get-Content summary.json -Raw | ConvertFrom-Json
            $parts = @()
            if ($summary.active_new_filings)       { $parts += "$($summary.active_new_filings) filings processed" }
            if ($summary.activations_succeeded)     { $parts += "$($summary.activations_succeeded) activated" }
            if ($summary.activations_needs_review)  { $parts += "$($summary.activations_needs_review) need review" }
            if ($summary.activations_failed)        { $parts += "$($summary.activations_failed) failed" }
            if ($parts.Count -gt 0) { $CommitMsg = "chore(scheduler): $($parts -join ', ')" }
        } catch {
            Log "Could not parse summary.json for commit message, using default."
        }

        git config user.name "sec-derivatives-bot"
        git config user.email "lukas.becker@infopro-digital.com"
        git commit -m $CommitMsg 2>&1 | Out-String | ForEach-Object { Log $_ }

        Log "Pushing to origin/master..."
        git push origin master 2>&1 | Out-String | ForEach-Object { Log $_ }
        Log "Push complete."
    } else {
        Log "No changes to commit."
    }

    Log "=== Scheduler run finished ==="

} catch {
    Log "FATAL ERROR: $_"
    Log $_.ScriptStackTrace
    exit 1
}

# Clean up old logs (keep last 30 days)
Get-ChildItem "$LogDir\scheduler_*.log" | Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } | Remove-Item -Force
