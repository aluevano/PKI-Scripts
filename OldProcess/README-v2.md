# Updated Scripts (v2) – 03 & 04

This bundle contains refactored versions of scripts **03** and **04** with clearer parameters,
stronger validation, verbose output, and better resilience across Windows Server 2012R2–2022.

## Files
- `03-Get-IssuedCertCount.ps1` – counts certificates by disposition (Issued by default).
- `04-Get-PublishedTemplates.ps1` – exports templates published on the CA.

## Highlights
- Explicit `.PARAMETER` help in comment-based help blocks (`Get-Help .\03-Get-IssuedCertCount.ps1 -Full`).
- Auto-detect local CA config if `-CAConfig` is not supplied.
- Convenience time filters: `-LastDays` or exact `-From`/`-To`.
- Robust `certutil` invocation with timeout and verbose echo of the command line.
- Script 04 can optionally use `-UseADCS` if the `ADCSAdministration` module is available.

## Quick Examples
```powershell
# Script 03 – last 30 days of issued certs on local CA
03-Get-IssuedCertCount.ps1 -LastDays 30 -OutCsv C:\PKI\Inventory\IssuedLast30.csv -Verbose

# Script 03 – denied requests between two dates on a specific CA
03-Get-IssuedCertCount.ps1 -CAConfig 'CAHOST\Corp-Issuing-CA' -Disposition 21 -From '2025-01-01' -To '2025-06-30' -OutCsv C:\PKI\Inventory\DeniedH1.csv -Verbose

# Script 04 – published templates via certutil
04-Get-PublishedTemplates.ps1 -OutCsv C:\PKI\Inventory\Templates.csv -Verbose

# Script 04 – use ADCSAdministration module if available
04-Get-PublishedTemplates.ps1 -UseADCS -OutCsv C:\PKI\Inventory\Templates.csv -Verbose
```
