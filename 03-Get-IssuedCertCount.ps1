[CmdletBinding()]
param(
    [string]$CAConfig,
    [datetime]$From,
    [datetime]$To,
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

function New-Restrict {
    param([datetime]$From,[datetime]$To)
    $parts = @('Disposition=20')
    if ($From) { $parts += 'NotBefore>=' + $From.ToString('yyyy-MM-dd HH:mm:ss') }
    if ($To)   { $parts += 'NotAfter<='  + $To.ToString('yyyy-MM-dd HH:mm:ss')  }
    $parts -join ' AND '
}

try {
    $cfg = Resolve-CAConfig -CAConfig $CAConfig
    $restrict = New-Restrict -From $From -To $To

    $args = @()
    if ($cfg) { $args += @('-config', $cfg) }
    $args += @('-view','-restrict', $restrict, '-out', 'RequestID')

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
    if ($p.ExitCode -ne 0) { throw "certutil failed: $stderr" }

    $count = ($stdout -split "`r?`n" | Where-Object { $_ -match 'Request\s*ID\s*:' }).Count

    $row = [pscustomobject]@{
        Timestamp = (Get-Date).ToString('s')
        CAConfig  = $cfg
        From      = if ($From) { $From.ToString('s') } else { '' }
        To        = if ($To) { $To.ToString('s') } else { '' }
        IssuedCount = $count
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote issued count to $OutCsv"
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
