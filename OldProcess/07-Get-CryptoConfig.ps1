[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$OutCsv
)

function Get-Reg([string]$path,[string]$name){
    try { (Get-ItemProperty -Path $path -ErrorAction Stop).$name } catch { $null }
}

try {
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { throw "CA configuration not found." }
    $active = (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
    if (-not $active) { throw "Unable to determine active CA instance." }
    $cspKey = Join-Path $base "$active\CSP"

    $row = [pscustomobject]@{
        Timestamp             = (Get-Date).ToString('s')
        Provider              = Get-Reg $cspKey 'Provider'
        ProviderType          = Get-Reg $cspKey 'ProviderType'
        CSPKeyContainer       = Get-Reg $cspKey 'KeyContainer'
        CNGPublicKeyAlgorithm = Get-Reg $cspKey 'CNGPublicKeyAlgorithm'
        CNGHashAlgorithm      = Get-Reg $cspKey 'CNGHashAlgorithm'
        MachineKeyset         = Get-Reg $cspKey 'MachineKeyset'
        LegacyProviderFlags   = Get-Reg $cspKey 'ProviderFlags'
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote crypto configuration to $OutCsv"
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
