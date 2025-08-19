[CmdletBinding()]
param(
    [string]$CAConfig,
    [Parameter(Mandatory=$true)]
    [string]$OutCsv
)

function Resolve-CAConfig {
    param([string]$CAConfig)
    if ($CAConfig) { return $CAConfig }
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { return $null }
    $active = (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
    if ($active) { return "$env:COMPUTERNAME\$active" }
    return $null
}

try {
    $cfg = Resolve-CAConfig -CAConfig $CAConfig
    $args = @()
    if ($cfg) { $args += @('-config', $cfg) }
    $args += @('-ping')

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'certutil.exe'
    $psi.Arguments = ($args -join ' ')
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    $p = [System.Diagnostics.Process]::Start($psi)
    $stdout = $p.StandardOutput.ReadToEnd()
    $stderr = $p.StandardError.ReadToEnd()
    $p.WaitForExit()

    $ok = $p.ExitCode -eq 0
    $row = [pscustomobject]@{
        Timestamp = (Get-Date).ToString('s')
        CAConfig  = $cfg
        Reachable = $ok
        Message   = if ($ok) { 'OK' } else { $stderr.Trim() }
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    if ($ok) { Write-Host "CA ping OK -> $OutCsv" } else { Write-Warning "CA ping failed -> $OutCsv" }
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
