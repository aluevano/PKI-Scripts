[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$OutCsv,
    [string]$RegBackupPath
)

function Get-LocalCAInstance {
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { return $null }
    (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
}

function Get-Reg([string]$path,[string]$name){
    try { (Get-ItemProperty -Path $path -ErrorAction Stop).$name } catch { $null }
}

try {
    $instance = Get-LocalCAInstance
    if (-not $instance) { throw "No local CA instance found in registry." }
    $regBase = "HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration\$instance"

    $keys = @(
        'CommonName','DSConfigDN','ValidityPeriod','ValidityPeriodUnits',
        'RenewalValidityPeriod','RenewalValidityPeriodUnits',
        'CRLPeriod','CRLPeriodUnits','CRLOverlapPeriod','CRLOverlapUnits',
        'ClockSkewMinutes','CACertPublicationURLs','CRLPublicationURLs'
    )

    $rows = foreach ($k in $keys) {
        $val = Get-Reg $regBase $k
        [pscustomobject]@{ Name = $k; Value = if ($val -is [array]) { $val -join '; ' } else { $val } }
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $rows | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote CA registry summary to $OutCsv"

    if (-not $RegBackupPath) {
        $RegBackupPath = Join-Path (Split-Path -Parent $OutCsv) 'ca-registry-backup.reg'
    }
    & reg.exe export "HKLM\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration" $RegBackupPath /y | Out-Null
    Write-Host "Exported full CA registry to $RegBackupPath"
}
catch {
    $err = [pscustomobject]@{
        Timestamp = (Get-Date).ToString('s')
        Error     = $_.Exception.Message
    }
    $err | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Error $_.Exception.Message
    exit 1
}
