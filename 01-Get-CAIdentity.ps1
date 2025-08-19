[CmdletBinding()]
param(
    # Optional: target a specific CA in the form HOST\CACommonName
    [string]$CAConfig,
    # CSV output path
    [Parameter(Mandatory=$true)]
    [string]$OutCsv
)

function Get-LocalCAConfig {
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { return $null }
    $active = (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
    if (-not $active) { 
        $active = (Get-ChildItem $base | Where-Object { $_.PSChildName -ne 'Templates' } | Select-Object -First 1).PSChildName 
    }
    if ($active) { return "$env:COMPUTERNAME\$active" }
    return $null
}

try {
    if (-not $CAConfig) { $CAConfig = Get-LocalCAConfig }
    $svc = Get-Service -Name certsvc -ErrorAction SilentlyContinue
    $svcStatus = if ($svc) { $svc.Status.ToString() } else { "NotInstalled" }
    $os = Get-CimInstance Win32_OperatingSystem
    $build = (Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion').CurrentBuild

    # Try to query CA info via certutil (best-effort)
    $caType = $null
    $caName = $null
    try {
        $ci = & certutil.exe '-cainfo' 2>&1
        foreach ($line in ($ci -split "`r?`n")) {
            if ($line -match '^\s*CA Type:\s*(.+)$') { $caType = $Matches[1].Trim() }
            if ($line -match '^\s*CA Name:\s*(.+)$') { $caName = $Matches[1].Trim() }
        }
    } catch {}

    $row = [pscustomobject]@{
        Timestamp     = (Get-Date).ToString('s')
        HostName      = $env:COMPUTERNAME
        CAConfig      = $CAConfig
        CAService     = $svcStatus
        OSVersion     = $os.Version
        OSCaption     = $os.Caption
        OSBuild       = $build
        CAType        = $caType
        CAName        = $caName
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote CA identity to $OutCsv"
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
