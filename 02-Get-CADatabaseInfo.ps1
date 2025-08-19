[CmdletBinding()]
param(
    [string]$CAConfig,
    [Parameter(Mandatory=$true)]
    [string]$OutCsv
)

function Get-RegValue([string]$path,[string]$name){
    try { (Get-ItemProperty -Path $path -ErrorAction Stop).$name } catch { $null }
}

function Get-LocalCAInstance {
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { return $null }
    (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
}

try {
    $instance = Get-LocalCAInstance
    $dbDir = $null; $logDir = $null
    if ($instance) {
        $regBase = "HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration\$instance"
        $dbDir  = Get-RegValue $regBase 'DBDirectory'
        $logDir = Get-RegValue $regBase 'LogDirectory'
    }

    if (-not $dbDir) { 
        # fallback to default
        $dbDir = Join-Path $env:SystemRoot 'System32\CertLog' 
    }
    if (-not $logDir) { $logDir = $dbDir }

    $edb = Join-Path $dbDir 'CA.edb'
    $edbSizeMB = $null
    if (Test-Path $edb) { $edbSizeMB = [math]::Round((Get-Item $edb).Length / 1MB, 2) }

    $logFiles = @()
    if (Test-Path $logDir) {
        $logFiles = Get-ChildItem -Path $logDir -Include *.log,*.jrs -File -ErrorAction SilentlyContinue
    }
    $logCount = $logFiles.Count
    $logTotalMB = [math]::Round(($logFiles | Measure-Object Length -Sum).Sum / 1MB, 2)

    $row = [pscustomobject]@{
        Timestamp         = (Get-Date).ToString('s')
        CAConfig          = if ($instance) { "$env:COMPUTERNAME\$instance" } else { $CAConfig }
        DBDirectory       = $dbDir
        LogDirectory      = $logDir
        EDBPath           = $edb
        EDBSizeMB         = $edbSizeMB
        LogFileCount      = $logCount
        LogTotalSizeMB    = $logTotalMB
        SystemDriveFreeMB = [math]::Round((Get-PSDrive -Name $env:SystemDrive.TrimEnd(':')).Free/1MB,0)
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote DB info to $OutCsv"
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
