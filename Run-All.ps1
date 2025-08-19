[CmdletBinding()]
param(
    [string]$CAConfig,
    [string]$OutputDir = "C:\PKI\Inventory"
)
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$ts = Get-Date -Format 'yyyyMMdd-HHmm'
$paths = @{
    IdentityCsv      = Join-Path $OutputDir "01-CAIdentity-$ts.csv"
    DbInfoCsv        = Join-Path $OutputDir "02-CADatabase-$ts.csv"
    IssuedCountCsv   = Join-Path $OutputDir "03-IssuedCount-$ts.csv"
    TemplatesCsv     = Join-Path $OutputDir "04-Templates-$ts.csv"
    RegSummaryCsv    = Join-Path $OutputDir "05-RegistrySummary-$ts.csv"
    CRLAIACsv        = Join-Path $OutputDir "06-CRL-AIA-$ts.csv"
    CryptoCsv        = Join-Path $OutputDir "07-CryptoConfig-$ts.csv"
    PingCsv          = Join-Path $OutputDir "08-HealthPing-$ts.csv"
}

$here = Split-Path -Parent $MyInvocation.MyCommand.Path

& (Join-Path $here '01-Get-CAIdentity.ps1')        -CAConfig $CAConfig -OutCsv $paths.IdentityCsv
& (Join-Path $here '02-Get-CADatabaseInfo.ps1')    -OutCsv $paths.DbInfoCsv
& (Join-Path $here '03-Get-IssuedCertCount.ps1')   -CAConfig $CAConfig -OutCsv $paths.IssuedCountCsv
& (Join-Path $here '04-Get-PublishedTemplates.ps1')-CAConfig $CAConfig -OutCsv $paths.TemplatesCsv
& (Join-Path $here '05-Export-CARegistrySummary.ps1') -OutCsv $paths.RegSummaryCsv
& (Join-Path $here '06-Get-CRL-AIA.ps1')           -OutCsv $paths.CRLAIACsv
& (Join-Path $here '07-Get-CryptoConfig.ps1')      -OutCsv $paths.CryptoCsv
& (Join-Path $here '08-Health-Ping.ps1')           -CAConfig $CAConfig -OutCsv $paths.PingCsv

Write-Host "All inventories written under $OutputDir"
