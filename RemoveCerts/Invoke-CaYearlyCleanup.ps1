<# 
.SYNOPSIS
  Delete AD CS CA database rows by yearly “buckets” up to a cutoff date, with parallelism, retries, and logging.

.DESCRIPTION
  For each year ≤ Cutoff.Year, this script builds a date boundary and invokes:
    certutil.exe -f -deleterow <BoundaryDate> <Table>
  Tables can be Request, Cert, and/or CRL. Work items (Table x Year) are executed with a controlled degree of parallelism.

  Compatible with Windows PowerShell 5.1 (WMF 5.1). No event handlers; Start-Process is used with redirection.

.PARAMETER Cutoff
  The final cutoff DateTime. All yearly buckets up to and including Cutoff.Year will be processed.
  For years < Cutoff.Year → boundary is 12/31/<year> 23:59:59. For year == Cutoff.Year → boundary is the exact Cutoff.

.PARAMETER Tables
  One or more of: Request, Cert, CRL. Defaults to all three.

.PARAMETER FromYear
  Earliest year to process. Defaults to [max(2000, Cutoff.Year - 15)].

.PARAMETER DegreeOfParallelism
  Number of concurrent deletions to run (default 2). Be conservative (2–3) to avoid DB contention.

.PARAMETER TimeoutSec
  Timeout per certutil invocation (0 = no timeout).

.PARAMETER MaxRetries
  Max retry attempts for ESENT version store exhaustion (-939523027). Default 4 (with backoff).

.PARAMETER LogDir
  Folder for per-batch stdout/stderr logs. Created if missing. Default: $env:TEMP\CA-Cleanup-Logs

.PARAMETER Preview
  Dry-run. Prints planned commands and exits (no deletes).

.PARAMETER ConfirmEachYear
  Prompt before executing each year boundary (ignored in -Preview).

.EXAMPLE
  # Clean data up to 12/31/2019, start from 2005, DOP=2
  .\Invoke-CaYearlyCleanup.ps1 -Cutoff '2019-12-31' -FromYear 2005 -DegreeOfParallelism 2

.EXAMPLE
  # Preview only (no deletes), show commands that would run
  .\Invoke-CaYearlyCleanup.ps1 -Cutoff '2019-12-31' -Preview -Verbose
#>
[CmdletBinding(SupportsShouldProcess=$false, PositionalBinding=$true)]
param(
    [Parameter(Mandatory=$true)]
    [datetime]$Cutoff,

    [Parameter()]
    [ValidateSet('Request','Cert','CRL')]
    [string[]]$Tables = @('Request','Cert','CRL'),

    [Parameter()]
    [ValidateRange(1900, 9999)]
    [int]$FromYear,

    [Parameter()]
    [ValidateRange(1,16)]
    [int]$DegreeOfParallelism = 2,

    [Parameter()]
    [ValidateRange(0,86400)]
    [int]$TimeoutSec = 0,

    [Parameter()]
    [ValidateRange(0,10)]
    [int]$MaxRetries = 4,

    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$LogDir = (Join-Path $env:TEMP 'CA-Cleanup-Logs'),

    [Parameter()]
    [switch]$Preview,

    [Parameter()]
    [switch]$ConfirmEachYear
)

# ----------------------------- Helpers -----------------------------

function Test-CertutilPresent {
    if (-not (Get-Command certutil.exe -ErrorAction SilentlyContinue)) {
        throw "certutil.exe not found. Install AD CS tools or run this on a CA server."
    }
}

# Format DateTime as en-US to keep certutil happy
function Format-DateForCertutil {
    param([datetime]$dt)
    $enUS = [System.Globalization.CultureInfo]::GetCultureInfo('en-US')
    return $dt.ToString('MM/dd/yyyy HH:mm:ss', $enUS)
}

# For years < Cutoff.Year → 12/31/<year> 23:59:59; for Cutoff.Year → exact Cutoff
function Get-YearBoundaryString {
    param([int]$Year, [datetime]$Cutoff)
    if ($Year -lt $Cutoff.Year) {
        $dt = [datetime]::new($Year,12,31,23,59,59)
        return (Format-DateForCertutil -dt $dt)
    } else {
        return (Format-DateForCertutil -dt $Cutoff)
    }
}

# Build the argument line for Start-Process (string, correctly quoted)
function Join-Args {
    param([string[]]$Args)
    return ($Args | ForEach-Object {
        if ($_ -match '\s') { '"' + ($_ -replace '"','""') + '"' } else { $_ }
    }) -join ' '
}

# ESENT version store exhaustion under heavy deletes
$ESENT_VersionStore = -939523027

# ----------------------------- Validation -----------------------------
try {
    Test-CertutilPresent

    if ($Cutoff -gt (Get-Date)) { throw "Cutoff ($Cutoff) is in the future. Provide a past date." }

    if (-not $FromYear) { $FromYear = [Math]::Max(2000, $Cutoff.Year - 15) }
    if ($FromYear -gt $Cutoff.Year) { throw "-FromYear ($FromYear) cannot be greater than Cutoff.Year ($($Cutoff.Year))." }

    if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Force -Path $LogDir | Out-Null }
}
catch {
    Write-Error $_.Exception.Message
    return
}

# Build yearly boundaries (inclusive up to Cutoff.Year)
$years = $FromYear..$Cutoff.Year
$work  = @()
foreach ($t in $Tables) {
    foreach ($y in $years) {
        $ds = Get-YearBoundaryString -Year $y -Cutoff $Cutoff
        $work += [pscustomobject]@{
            Table      = $t
            Year       = $y
            DateString = $ds
            Attempt    = 0
        }
    }
}

if ($Preview) {
    Write-Host "=== PREVIEW MODE ==="
    Write-Host "Cutoff: $Cutoff  | FromYear: $FromYear  | Tables: $($Tables -join ',')  | DOP: $DegreeOfParallelism"
    foreach ($item in $work) {
        $cmd = "certutil.exe " + (Join-Args @('-f','-deleterow', $item.DateString, $item.Table))
        Write-Host ("{0}  {1}" -f $item.Year, $cmd)
    }
    return
}

# Optional confirmation per year boundary
if ($ConfirmEachYear) {
    $yearsToRun = @()
    foreach ($year in $years) {
        $ans = Read-Host "Proceed with deletes for year $year ? (Y/N)"
        if ($ans -in @('Y','y','Yes','yes')) { $yearsToRun += $year }
    }
    if ($yearsToRun.Count -eq 0) {
        Write-Warning "No years selected. Exiting."
        return
    }
    $work = $work | Where-Object { $_.Year -in $yearsToRun }
}

# ----------------------------- Scheduler (Parallel with Throttling) -----------------------------

$results = New-Object System.Collections.Generic.List[object]
$queue   = New-Object System.Collections.Queue
$work | ForEach-Object { $queue.Enqueue($_) }
$active = @()  # items: @{ Proc = <process>; Item = <workItem>; Attempt = <n>; OutPath=..; ErrPath=..; Cmd=..; StartTime=.. }

function Start-WorkItemAsync {
    param([pscustomobject]$item)

    $attempt = $item.Attempt + 1
    $ts      = Get-Date -Format 'yyyyMMdd-HHmmss'
    $safeDt  = ($item.DateString -replace '[^\d]','')
    $outFile = Join-Path $LogDir ("deleterow_{0}_{1}_att{2}_{3}.out.txt" -f $item.Table,$safeDt,$attempt,$ts)
    $errFile = Join-Path $LogDir ("deleterow_{0}_{1}_att{2}_{3}.err.txt" -f $item.Table,$safeDt,$attempt,$ts)

    $argLine = Join-Args @('-f','-deleterow', $item.DateString, $item.Table)

    $proc = Start-Process -FilePath 'certutil.exe' -ArgumentList $argLine -NoNewWindow `
        -RedirectStandardOutput $outFile -RedirectStandardError $errFile -PassThru

    @{
        Proc      = $proc
        Item      = $item
        Attempt   = $attempt
        OutPath   = $outFile
        ErrPath   = $errFile
        Cmd       = "certutil.exe $argLine"
        StartTime = Get-Date
    }
}

while ($queue.Count -gt 0 -or $active.Count -gt 0) {

    # Fill until throttle
    while ($active.Count -lt $DegreeOfParallelism -and $queue.Count -gt 0) {
        $next = $queue.Dequeue()
        $active += (Start-WorkItemAsync -item $next)
    }

    # Poll for completions / timeouts
    Start-Sleep -Milliseconds 250
    $still = @()
    foreach ($a in $active) {
        $p = $a.Proc
        $timedOut = $false

        if ($TimeoutSec -gt 0 -and -not $p.HasExited) {
            $elapsed = ((Get-Date) - $a.StartTime).TotalSeconds
            if ($elapsed -gt $TimeoutSec) {
                try { $p.Kill() } catch {}
                $timedOut = $true
            }
        }

        if (-not $p.HasExited -and -not $timedOut) { $still += $a; continue }

        $exit = if ($timedOut) { 1460 } else { $p.ExitCode }  # 1460 = ERROR_TIMEOUT
        $dur  = [Math]::Round(((Get-Date) - $a.StartTime).TotalSeconds,2)

        $res = [pscustomobject]@{
            Table       = $a.Item.Table
            Year        = $a.Item.Year
            DateString  = $a.Item.DateString
            Attempt     = $a.Attempt
            ExitCode    = $exit
            DurationSec = $dur
            StdOutPath  = $a.OutPath
            StdErrPath  = $a.ErrPath
            Command     = $a.Cmd
        }
        $results.Add($res) | Out-Null

        if ($exit -eq $ESENT_VersionStore -and $a.Attempt -lt $MaxRetries) {
            $delay = @(3,10,30,60,120)[$a.Attempt - 1]; if (-not $delay) { $delay = 120 }
            Write-Warning ("{0}/{1}: Exit {2}; retrying in {3}s (attempt {4}/{5})" -f $a.Item.Table,$a.Item.Year,$exit,$delay,($a.Attempt+1),$MaxRetries)
            Start-Sleep -Seconds $delay
            # Re-queue with incremented attempt
            $queue.Enqueue([pscustomobject]@{
                Table      = $a.Item.Table
                Year       = $a.Item.Year
                DateString = $a.Item.DateString
                Attempt    = $a.Attempt
            })
        }
        elseif ($exit -ne 0) {
            Write-Warning ("{0}/{1}: certutil failed with exit {2}. See logs: {3} / {4}" -f $a.Item.Table,$a.Item.Year,$exit,$a.OutPath,$a.ErrPath)
        }
    }
    $active = $still
}

# ----------------------------- Summary -----------------------------

$byTable = $results | Group-Object Table | Sort-Object Name
foreach ($g in $byTable) {
    $ok   = ($g.Group | Where-Object { $_.ExitCode -eq 0 }).Count
    $fail = ($g.Group | Where-Object { $_.ExitCode -ne 0 }).Count
    Write-Host ("[{0}] OK: {1}  Fail: {2}" -f $g.Name, $ok, $fail)
}

$summaryCsv = Join-Path $LogDir ("CleanupSummary_{0}_{1}-{2}.csv" -f ($Tables -join ''), $FromYear, $Cutoff.Year)
$results | Select-Object Table,Year,DateString,Attempt,ExitCode,DurationSec,StdOutPath,StdErrPath,Command |
    Export-Csv -Path $summaryCsv -NoTypeInformation -Encoding UTF8

Write-Host "Summary written: $summaryCsv"
Write-Host "Logs folder: $LogDir"
