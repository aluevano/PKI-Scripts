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
    $args += @('-catemplates','-v')

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

    $lines = $stdout -split "`r?`n"
    $records = @()
    $current = $null

    foreach ($line in $lines) {
        if ($line -match '^\s*Template\s*:\s*(.+?)\s*\(([^)]+)\)\s*$') {
            if ($current) { $records += [pscustomobject]$current }
            $current = [ordered]@{
                TemplateDisplayName = $Matches[1].Trim()
                OID                 = $Matches[2].Trim()
                SchemaVersion       = $null
                MajorVersion        = $null
                MinorVersion        = $null
            }
            continue
        }
        if (-not $current) { continue }
        if ($line -match '^\s*Schema Version\s*:\s*(\d+)\s*$') { $current.SchemaVersion = [int]$Matches[1]; continue }
        if ($line -match '^\s*Major Version\s*:\s*(\d+)\s*$')  { $current.MajorVersion  = [int]$Matches[1]; continue }
        if ($line -match '^\s*Minor Version\s*:\s*(\d+)\s*$')  { $current.MinorVersion  = [int]$Matches[1]; continue }
    }
    if ($current) { $records += [pscustomobject]$current }

    if (-not $records) {
        $records = @([pscustomobject]@{ TemplateDisplayName=''; OID=''; SchemaVersion=''; MajorVersion=''; MinorVersion=''})
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $records | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote templates to $OutCsv"
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
