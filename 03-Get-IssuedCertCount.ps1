<#
.SYNOPSIS
  Count certificates in a Windows CA database (Issued by default) and write a single-row CSV.

.DESCRIPTION
  Uses only built-in 'certutil.exe' to query the CA database. By default counts Disposition=20 (Issued).
  You can change the disposition with -Disposition or provide convenience windows via -LastDays/-From/-To.

.PARAMETER OutCsv
  FULL path of the CSV file to write. The parent folder will be created if missing.

.PARAMETER CAConfig
  Optional CA config string in the form "HOST\CA Common Name". If omitted, the local CA instance
  will be auto-detected from HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration\Active

.PARAMETER Disposition
  Certificate request disposition to count. Common values: 20=Issued (default), 21=Denied, 9=Pending, 22=Revoked.
  You can also pass 'All' to count ALL rows regardless of disposition (useful for sanity checks).

.PARAMETER LastDays
  If specified, limits to certificates with NotBefore >= (Now - LastDays). Mutually exclusive with -From.

.PARAMETER From
  Start of validity window (NotBefore >= From). Mutually exclusive with -LastDays.

.PARAMETER To
  End of validity window (NotAfter <= To). If omitted, no upper bound is applied.

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -OutCsv C:\PKI\Inventory\IssuedCount.csv

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -CAConfig 'CAHOST\Corp-Issuing-CA' -LastDays 30 -OutCsv C:\PKI\Inventory\IssuedLast30.csv

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -Disposition 21 -From '2025-01-01' -To '2025-06-30' -OutCsv C:\PKI\Inventory\DeniedH1.csv
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true, HelpMessage="Full path of the CSV file to write.")]
    [ValidateNotNullOrEmpty()]
    [string]$OutCsv,

    [Parameter(HelpMessage='CA config "HOST\CA Common Name". If omitted, attempts local auto-detection.')]
    [string]$CAConfig,

    [Parameter(HelpMessage='Disposition code or "All". 20=Issued (default), 21=Denied, 9=Pending, 22=Revoked.')]
    [ValidatePattern('^(All|[0-9]+)$')]
    [string]$Disposition = '20',

    [Parameter(HelpMessage='Count items with NotBefore >= (Now - LastDays). Mutually exclusive with -From.')]
    [ValidateRange(1, 36500)]
    [int]$LastDays,

    [Parameter(HelpMessage='Start of validity (NotBefore >= From). Mutually exclusive with -LastDays.')]
    [datetime]$From,

    [Parameter(HelpMessage='End of validity (NotAfter <= To).')]
    [datetime]$To
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

function New-RestrictString {
    param([string]$Disposition,[datetime]$From,[datetime]$To,[int]$LastDays)
    $parts = @()
    if ($Disposition -ne 'All') { $parts += "Disposition=$Disposition" }
    if ($LastDays) { $From = (Get-Date).AddDays(-$LastDays) }
    if ($From) { $parts += 'NotBefore>=' + $From.ToString('yyyy-MM-dd HH:mm:ss') }
    if ($To)   { $parts += 'NotAfter<='  + $To.ToString('yyyy-MM-dd HH:mm:ss')  }
    if ($parts.Count -eq 0) { return $null } else { return ($parts -join ' AND ') }
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
    if ($PSBoundParameters.ContainsKey('From') -and $PSBoundParameters.ContainsKey('LastDays')) {
        throw "Use either -From or -LastDays, not both."
    }
    if ($PSBoundParameters.ContainsKey('From') -and $PSBoundParameters.ContainsKey('To')) {
        if ($From -gt $To) { throw "-From cannot be later than -To." }
    }

    $cfg = Resolve-CAConfig -Explicit $CAConfig
    $restrict = New-RestrictString -Disposition $Disposition -From $From -To $To -LastDays $LastDays

    $args = @()
    if ($cfg) { $args += @('-config', ('"'+$cfg+'"')) }
    $args += @('-view')
    if ($restrict) { $args += @('-restrict', ('"'+$restrict+'"')) }
    $args += @('-out', 'RequestID')

    $out = Invoke-Certutil -ArgumentList $args

    # Count lines that contain Request ID (language-agnostic: look for "Request" and "ID" with colon in between)
    $count = 0
    foreach ($line in ($out -split "`r?`n")) {
        if ($line -match 'Request\s*ID\s*:') { $count++ }
    }

    $row = [pscustomobject]@{
        Timestamp   = (Get-Date).ToString('s')
        CAConfig    = $cfg
        Disposition = $Disposition
        From        = if ($From) { $From.ToString('s') } elseif ($LastDays){ (Get-Date).AddDays(-$LastDays).ToString('s') } else { '' }
        To          = if ($To) { $To.ToString('s') } else { '' }
        Count       = $count
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote count= $count to $OutCsv"
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
