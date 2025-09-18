<#
.SYNOPSIS
  Export the list of certificate templates PUBLISHED on a CA to CSV.

.DESCRIPTION
  Parses 'certutil -catemplates -v' (optionally targeted via -CAConfig) and extracts template
  display name, internal name (if present), OID and version numbers. No RSAT/AD modules needed.
  Optionally, if you set -UseADCS and the ADCSAdministration module is available, the script
  will use Get-CATemplate for richer data while emitting the same core columns.

.PARAMETER OutCsv
  FULL path of the CSV file to write. The parent folder will be created if missing.

.PARAMETER CAConfig
  Optional CA config string in the form "HOST\CA Common Name". If omitted, uses local CA instance.

.PARAMETER UseADCS
  Switch. If specified and module ADCSAdministration is available, uses Get-CATemplate for data.

.EXAMPLE
  .\04-Get-PublishedTemplates.ps1 -OutCsv C:\PKI\Inventory\Templates.csv

.EXAMPLE
  .\04-Get-PublishedTemplates.ps1 -CAConfig 'CAHOST\Corp-Issuing-CA' -OutCsv C:\PKI\Inventory\Templates.csv

.EXAMPLE
  .\04-Get-PublishedTemplates.ps1 -UseADCS -OutCsv C:\PKI\Inventory\Templates.csv
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, HelpMessage="Full path of the CSV file to write.")]
    [ValidateNotNullOrEmpty()]
    [string]$OutCsv,

    [Parameter(HelpMessage='CA config "HOST\CA Common Name". If omitted, attempts local auto-detection.')]
    [string]$CAConfig,

    [Parameter(HelpMessage='Use ADCSAdministration module if available for richer data. Optional.')]
    [switch]$UseADCS
)

#region Helpers
function Test-CertutilPresent {
    $p = Get-Command certutil.exe -ErrorAction SilentlyContinue
    if (-not $p) { throw "certutil.exe not found in PATH. Install AD CS tools or run on the CA host." }
}
function Resolve-CAConfig {
    param([string]$Explicit)
    if ($Explicit) { return $Explicit }
    $base = 'HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration'
    if (-not (Test-Path $base)) { return $null }
    $active = (Get-ItemProperty -Path $base -Name 'Active' -ErrorAction SilentlyContinue).Active
    if ($active) { return "$env:COMPUTERNAME\$active" }
    return $null
}
function Invoke-Certutil {
    param([string[]]$ArgumentList,[int]$TimeoutSec=120)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'certutil.exe'
    $psi.Arguments = ($ArgumentList | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() }) -join ' '
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    Write-Verbose "Running: certutil.exe $($psi.Arguments)"
    $proc = [System.Diagnostics.Process]::Start($psi)
    if (-not $proc.WaitForExit($TimeoutSec*1000)) {
        try { $proc.Kill() } catch {}
        throw "certutil timed out after $TimeoutSec seconds."
    }
    $stdout = $proc.StandardOutput.ReadToEnd()
    $stderr = $proc.StandardError.ReadToEnd()
    if ($proc.ExitCode -ne 0) { throw "certutil failed ($($proc.ExitCode)): $stderr" }
    return $stdout
}
#endregion Helpers

try {
    Test-CertutilPresent
    $cfg = Resolve-CAConfig -Explicit $CAConfig

    $rows = @()

    if ($UseADCS -and (Get-Module -ListAvailable -Name ADCSAdministration)) {
        Import-Module ADCSAdministration -ErrorAction Stop
        $caShort = $null
        if ($cfg -and $cfg.Contains('\')) { $caShort = $cfg.Split('\',2)[1] }
        $templates = if ($caShort) { Get-CATemplate -CA $caShort } else { Get-CATemplate }
        foreach ($t in $templates) {
            $rows += [pscustomobject]@{
                TemplateDisplayName = $t.DisplayName
                TemplateInternalName = $t.Name
                OID = $t.OID.Value
                SchemaVersion = $t.SchemaVersion
                MajorVersion = $t.MajorVersion
                MinorVersion = $t.MinorVersion
            }
        }
    } else {
        $args = @()
        if ($cfg) { $args += @('-config', ('"'+$cfg+'"')) }
        $args += @('-catemplates','-v')

        $out = Invoke-Certutil -ArgumentList $args
        $current = $null
        foreach ($line in ($out -split "`r?`n")) {
            # Match "Template : Display Name (1.2.3.4...)"
            if ($line -match '^\s*Template\s*:\s*(.+?)\s*\(([^)]+)\)\s*$') {
                if ($current) { $rows += [pscustomobject]$current }
                $current = [ordered]@{
                    TemplateDisplayName = $Matches[1].Trim()
                    TemplateInternalName = $null
                    OID = $Matches[2].Trim()
                    SchemaVersion = $null
                    MajorVersion = $null
                    MinorVersion = $null
                }
                continue
            }
            if (-not $current) { continue }
            # Internal name
            if ($line -match '^\s*Template internal name\s*:\s*(.+?)\s*$') { $current.TemplateInternalName = $Matches[1].Trim(); continue }
            # Versions
            if ($line -match '^\s*Schema Version\s*:\s*(\d+)\s*$') { $current.SchemaVersion = [int]$Matches[1]; continue }
            if ($line -match '^\s*Major Version\s*:\s*(\d+)\s*$')  { $current.MajorVersion  = [int]$Matches[1]; continue }
            if ($line -match '^\s*Minor Version\s*:\s*(\d+)\s*$')  { $current.MinorVersion  = [int]$Matches[1]; continue }
        }
        if ($current) { $rows += [pscustomobject]$current }
    }

    if (-not $rows -or $rows.Count -eq 0) {
        $rows = @([pscustomobject]@{ TemplateDisplayName=''; TemplateInternalName=''; OID=''; SchemaVersion=''; MajorVersion=''; MinorVersion='' })
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $rows | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote $($rows.Count) template rows to $OutCsv"
}
catch {
    $err = [pscustomobject]@{ Timestamp=(Get-Date).ToString('s'); Error=$_.Exception.Message }
    try {
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
        $err | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    } catch {}
    Write-Error $_.Exception.Message
    exit 1
}
