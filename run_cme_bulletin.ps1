# CME Interest-Rate Bulletin tracker — local scheduled job
# Runs as a Windows Scheduled Task on weekday mornings (after CME finalises the
# prior session's Daily Bulletin, ~10:00 CT).
#
# Ingest order each run:
#   1. Direct download (primary) — `pull` fetches the bulletin from THIS machine's
#      network: plain request first, then an automatic headless-Chromium fallback
#      for Akamai's bot check.  This is the hands-off path (no manual upload).
#   2. Inbox drain (safety net) — any *.pdf you happen to drop into inbox\cme\ is
#      parsed with `pull --file`, then moved to inbox\cme\processed\.
#   3. Commit & push any new data under data/cme/.
#
# One-time setup on this machine:
#   python -m pip install -r requirements.txt
#   # Uses your installed Chrome/Edge — no browser download needed. Only if
#   # neither is present: python -m playwright install chromium
#
# The download drives a REAL, HEADED browser (a window briefly opens), because
# CME blocks headless/scripted clients. So register the task to run only while
# you're logged on:
#   $Action  = New-ScheduledTaskAction -Execute "powershell.exe" `
#       -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Users\lukasbecker\claudeprojects\sec-derivatives\run_cme_bulletin.ps1`""
#   $Trigger = New-ScheduledTaskTrigger -Weekly `
#       -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 3:00PM
#   Register-ScheduledTask -TaskName "CME IR Bulletin" -Action $Action -Trigger $Trigger
#       # (default "run only when user is logged on" is what we want here)
#
# Note: CME's Data Terms of Use restrict automated access. This runs on your own
# machine for content you're entitled to view; the compliant alternatives (CME's
# free Daily Bulletin email subscription, or a licensed CME DataMine feed) remain
# available if you prefer.

$ErrorActionPreference = "Stop"
$RepoDir  = "C:\Users\lukasbecker\claudeprojects\sec-derivatives"
$InboxDir = "$RepoDir\inbox\cme"
$DoneDir  = "$InboxDir\processed"
$LogDir   = "$RepoDir\logs"
$LogFile  = "$LogDir\cme_bulletin_$(Get-Date -Format 'yyyy-MM-dd_HHmmss').log"

foreach ($d in @($InboxDir, $DoneDir, $LogDir)) {
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

function Log { param([string]$Msg)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "$ts  $Msg" | Tee-Object -FilePath $LogFile -Append
}

try {
    Log "=== CME bulletin run started ==="
    Set-Location $RepoDir

    Log "Pulling latest from origin/master..."
    git pull origin master 2>&1 | Out-String | ForEach-Object { Log $_ }

    Log "Installing dependencies..."
    python -m pip install -q -r requirements.txt 2>&1 | Out-String | ForEach-Object { Log $_ }

    # 1. Direct download (primary, hands-off) --------------------------------
    Log "Downloading today's bulletin from CME (requests, then headless browser)..."
    python -m src.cme_bulletin pull --verbose 2>&1 | Out-String | ForEach-Object { Log $_ }
    if ($LASTEXITCODE -ne 0) {
        Log "WARNING: direct download failed (CME may have blocked this network). Drop today's PDF into $InboxDir to ingest it, or check the log above."
    }

    # 2. Drain the inbox (safety net for any manually dropped PDFs) -----------
    $pdfs = Get-ChildItem "$InboxDir\*.pdf" -ErrorAction SilentlyContinue
    if ($pdfs) {
        foreach ($pdf in $pdfs) {
            Log "Parsing dropped file $($pdf.Name)..."
            python -m src.cme_bulletin pull --file $pdf.FullName --verbose 2>&1 |
                Out-String | ForEach-Object { Log $_ }
            if ($LASTEXITCODE -eq 0) {
                Move-Item $pdf.FullName (Join-Path $DoneDir $pdf.Name) -Force
            } else {
                Log "WARNING: failed to parse $($pdf.Name); leaving it in the inbox."
            }
        }
    }

    # 3. Commit & push -------------------------------------------------------
    Log "Staging data/cme/ ..."
    git add data/cme/

    git diff --staged --quiet 2>&1; $changed = $LASTEXITCODE
    if ($changed -ne 0) {
        git config user.name "sec-derivatives-bot"
        git config user.email "lukas.becker@infopro-digital.com"
        git commit -m "chore(cme): daily bulletin volume & open interest" 2>&1 |
            Out-String | ForEach-Object { Log $_ }
        Log "Pushing to origin/master..."
        git push origin master 2>&1 | Out-String | ForEach-Object { Log $_ }
        Log "Push complete."
    } else {
        Log "No new bulletin data to commit."
    }

    Log "=== CME bulletin run finished ==="

} catch {
    Log "FATAL ERROR: $_"
    Log $_.ScriptStackTrace
    exit 1
}

# Clean up old logs (keep last 30 days)
Get-ChildItem "$LogDir\cme_bulletin_*.log" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-30) } | Remove-Item -Force
