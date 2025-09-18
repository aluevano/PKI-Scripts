# 03-Get-IssuedCertCount.ps1 (v2.1)

**Fix for:** `Cannot process argument transformation on parameter 'From'. Cannot convert null to type "System.DateTime".`

## What changed
- `-From` and `-To` are now **strings**, parsed safely inside the script (so passing `$null` or an empty value no longer triggers a binding error).
- Added `-Preview` to show the computed `certutil` command line without executing (great for dry runs).
- Introduced parameter sets (`LastDays` vs `Range`) to reduce accidental misuse.
- Clearer help (`Get-Help .\03-Get-IssuedCertCount.ps1 -Full`).

## Examples
```powershell
# Local CA, last 7 days, preview only (no certutil execution)
.\03-Get-IssuedCertCount.ps1 -LastDays 7 -OutCsv C:	emp\out.csv -Preview -Verbose

# Remote CA, explicit date range
.\03-Get-IssuedCertCount.ps1 -CAConfig 'CAHOST\Corp-Issuing-CA' `
  -From '2025-01-01' -To '2025-06-30' -OutCsv C:	emp
ange.csv -Verbose

# All rows regardless of disposition
.\03-Get-IssuedCertCount.ps1 -Disposition All -OutCsv C:	empll.csv
```
