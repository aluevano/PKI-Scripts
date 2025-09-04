# Export-CARecords-Streamer.ps1

**Purpose:** Export **all certificate records** from a Microsoft CA to CSV **without using dates**,
with **constant memory** usage via streaming and **flush-every-N** records. Optionally, walk
the database by **RequestID windows** to keep each certutil process short.


**Default Columms:** RequestID, SerialNumber, CommonName, RequesterName, CertificateTemplate, NotBefore, NotAfter, Thumbprint, UPN, SAN, SubjectDN 

**Pick your own columns**
```powershell 
$cols = 'RequestID','SerialNumber','CommonName','RequesterName','CertificateTemplate','NotBefore','NotAfter','Thumbprint'
.\Export-CARecords-Streamer.ps1 -OutCsv C:\PKI\Exports\Minimal.csv -Properties $cols -FlushEvery 1500 -Verbose

```

## Key features
- Streams `certutil -view` output; **no giant strings in memory**.
- `-FlushEvery` (default **1000**) writes and clears the in-memory chunk.
- Optional **RequestID batching**: `-RequestIDStart`, `-RequestIDEnd`, `-RequestIDBatchSize`.
- Choose **which columns** to export via `-Properties` (friendly names).


## Notes & tuning
- Timeouts: -TimeoutSec 0 (default) disables per-process timeout — good for very large exports. If you prefer guardrails, set a value (e.g., 3600).
- Memory safety: The script parses lines as they stream and flushes chunks, so memory stays flat even for tens of millions of rows.
- Columns included by default: RequestID, SerialNumber, CommonName, RequesterName, CertificateTemplate, NotBefore, NotAfter, Thumbprint, UPN, SAN, SubjectDN.
- CA selection: omit -CAConfig to auto-detect the local CA, or pass "HOST\CA Common Name".


## Quick starts
```powershell
# Export all Issued certs to CSV, flushing every 1000 rows
.\Export-CARecords-Streamer.ps1 -OutCsv C:\PKI\Exports\AllIssued.csv -Disposition 20 -FlushEvery 1000 -Verbose

# RequestID window batching: 50k IDs per batch
.\Export-CARecords-Streamer.ps1 -RequestIDStart 1 -RequestIDEnd 1500000 -RequestIDBatchSize 50000 `
  -OutCsv C:\PKI\Exports\AllIssued.csv -FlushEvery 2000 -Verbose
```

# Export-CARecords-Streamer.ps1 (v1.2)

**What’s fixed**
- Removed **all** `.ContainsKey()` calls (which fail on `OrderedDictionary`). We now use `.Contains()` everywhere it matters.
- Continued-line handling uses the last seen key robustly.
- No other non-existent methods are referenced.

## Quick usage
```powershell
# Export all Issued certs (no dates), flush every 1000 rows
.\Export-CARecords-Streamer.ps1 -OutCsv C:\PKI\Exports\AllIssued.csv -Disposition 20 -FlushEvery 1000 -Verbose

# RequestID-batched export: 50k IDs per batch
.\Export-CARecords-Streamer.ps1 -RequestIDStart 1 -RequestIDEnd 1500000 -RequestIDBatchSize 50000 `
  -OutCsv C:\PKI\Exports\AllIssued.csv -FlushEvery 2000 -Verbose

# Minimal columns
$cols = 'RequestID','SerialNumber','NotBefore','NotAfter','Thumbprint'
.\Export-CARecords-Streamer.ps1 -OutCsv C:\PKI\Exports\Minimal.csv -Properties $cols -FlushEvery 1500 -Verbose

