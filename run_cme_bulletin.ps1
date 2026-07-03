# CME Interest-Rate Bulletin tracker — local scheduled job
# Runs as a Windows Scheduled Task on weekday mornings (after CME finalises the
# prior session's Daily Bulletin, ~10:00 CT).
#
# Ingest order each run:
#   1. Drain the inbox — any *.pdf you dropped into inbox\cme\ is parsed with
#      `pull --file`, then moved to inbox\cme\processed\.  This always works and
#      is the compliant path: you download the bulletin yourself, drop it in.
#   2. Best-effort direct download — `pull` tries the CME URL from THIS machine's
#      network.  If CME returns HTTP 403 (blocked / ToU) the job logs it and
#      carries on; it does not fail the run.
#   3. Commit & push any new data under data/cme/.
#
# One-time setup (register the task, weekdays 15:00 UTC — adjust as needed):
#   $Action  = New-ScheduledTaskAction -Execute "powershell.exe" `
#       -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Users\lukasbecker\claudeprojects\sec-derivatives\run_cme_bulletin.ps1`""
#   $Trigger = New-ScheduledTaskTrigger -Weekly `
#       -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 3:00PM
#   Register-ScheduledTask -TaskName "CME IR Bulletin" -Action $Action -Trigger $Trigger

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

    # 1. Drain the inbox -----------------------------------------------------
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
    } else {
        Log "Inbox empty."
    }

    # 2. Best-effort direct download ----------------------------------------
    Log "Attempting direct download from CME (best effort)..."
    python -m src.cme_bulletin pull --verbose 2>&1 | Out-String | ForEach-Object { Log $_ }
    if ($LASTEXITCODE -ne 0) {
        Log "Direct download unavailable (CME likely returned 403 / blocked). Drop today's PDF into $InboxDir to ingest it next run."
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
