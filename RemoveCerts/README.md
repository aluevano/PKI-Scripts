# Invoke-CaYearlyCleanup.ps1

Cleanup helper for **Microsoft AD CS** databases that deletes old rows in **yearly batches** up to a **cutoff date**.

- Deletes **Request**, **Cert**, and/or **CRL** tables (textual names; no numeric masks)
- Executes **in parallel** with a configurable throttle (safe on WMF 5.1)
- **Retries** ESENT version-store errors with backoff
- **Logs** stdout/stderr per batch and writes a **CSV summary**

## Requirements
- Windows Server with **AD CS tools** and access to `certutil.exe`
- Windows PowerShell **5.1** (or newer)

## Parameters
- `-Cutoff [DateTime]` (required): final cutoff date/time.
  - For each year `< Cutoff.Year`, boundary is `12/31/<year> 23:59:59`.
  - For `year == Cutoff.Year`, boundary is the exact cutoff.
- `-Tables Request,Cert,CRL` (optional; default all three)
- `-FromYear [int]` (optional; default `max(2000, Cutoff.Year-15)`)
- `-DegreeOfParallelism [1..16]` (optional; default `2`)
- `-TimeoutSec [0..86400]` (optional; per-invocation timeout; default `0` = none)
- `-MaxRetries [0..10]` (optional; default `4`, for ESENT `-939523027`)
- `-LogDir [path]` (optional; default `%TEMP%\CA-Cleanup-Logs`)
- `-Preview` (switch): prints the exact commands and exits.
- `-ConfirmEachYear` (switch): prompt per year before executing.

## Examples

### Preview (no deletes)
```powershell
.\Invoke-CaYearlyCleanup.ps1 -Cutoff '2019-12-31' -Preview -Verbose
```

### Run cleanup with parallelism
```powershell
.\Invoke-CaYearlyCleanup.ps1 `
  -Cutoff '2019-12-31' `
  -FromYear 2005 `
  -Tables Request,Cert,CRL `
  -DegreeOfParallelism 2 `
  -TimeoutSec 3600 `
  -MaxRetries 4 `
  -Verbose
```

### Only Requests and Certs
```powershell
.\Invoke-CaYearlyCleanup.ps1 -Cutoff '2018-12-31' -Tables Request,Cert -FromYear 2000 -DegreeOfParallelism 2
```

## What to expect
- For each (Table × Year) job, you’ll get an `.out.txt` and `.err.txt` file in `-LogDir`.
- On completion, a `CleanupSummary_<tables>_<from>-<to>.csv` is written in `-LogDir` summarizing exit codes and durations.
- The script uses **en-US** date formatting (`MM/dd/yyyy HH:mm:ss`) which `certutil` parses reliably.

## Safety notes
- Always take a **full CA database backup** (and System State) before deleting rows.
- Start with **`-Preview`**, then run with a **low** `-DegreeOfParallelism` (e.g., `2`).
- If you see exit `-939523027`, that’s ESENT version-store exhaustion; the script will automatically retry with backoff.
