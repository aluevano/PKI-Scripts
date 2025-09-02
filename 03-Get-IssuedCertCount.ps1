<#
.SYNOPSIS
  Count certificates in a Windows CA database (Issued by default) and write a single-row CSV.

.DESCRIPTION
  Uses built-in 'certutil.exe' to query the CA database. By default counts Disposition=20 (Issued).
  You can change the disposition with -Disposition or provide convenience time windows via -LastDays
  or explicit -From/-To. Dates are accepted as *strings* and parsed safely to avoid binding errors.

.PARAMETER OutCsv
  FULL path of the CSV file to write. The parent folder will be created if missing.

.PARAMETER CAConfig
  Optional CA config string in the form "HOST\CA Common Name". If omitted, the local CA instance
  will be auto-detected from HKLM:\SYSTEM\CurrentControlSet\Services\CertSvc\Configuration\Active

.PARAMETER Disposition
  Certificate request disposition to count. Common values: 20=Issued (default), 21=Denied, 9=Pending, 22=Revoked.
  Use 'All' to count ALL rows regardless of disposition.

.PARAMETER LastDays
  If specified, limits to certificates with NotBefore >= (Now - LastDays).
  Mutually exclusive with -From/-To.

.PARAMETER From
  (String) Start of validity window (NotBefore >= From). Accepts common formats, e.g.:
  '2025-01-01', '2025-01-01 13:30', '01/01/2025', etc. Leave blank or omit to use no lower bound.
  Mutually exclusive with -LastDays.

.PARAMETER To
  (String) End of validity window (NotAfter <= To). Accepts the same formats as -From.
  Leave blank or omit to use no upper bound.

.PARAMETER Preview
  If set, the script prints the computed certutil command and exits without running it.

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -OutCsv C:\PKI\Inventory\IssuedCount.csv

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -CAConfig 'CAHOST\Corp-Issuing-CA' -LastDays 30 -OutCsv C:\PKI\Inventory\IssuedLast30.csv

.EXAMPLE
  .\03-Get-IssuedCertCount.ps1 -Disposition 21 -From '2025-01-01' -To '2025-06-30' -OutCsv C:\PKI\Inventory\DeniedH1.csv
#>
[CmdletBinding(DefaultParameterSetName='None')]
param(
    [Parameter(Mandatory=$true, HelpMessage="Full path of the CSV file to write.")]
    [ValidateNotNullOrEmpty()]
    [string]$OutCsv,

    [Parameter(HelpMessage='CA config "HOST\CA Common Name". If omitted, attempts local auto-detection.')]
    [string]$CAConfig,

    [Parameter(HelpMessage='Disposition code or "All". 20=Issued (default), 21=Denied, 9=Pending, 22=Revoked.')]
    [ValidatePattern('^(All|[0-9]+)$')]
    [string]$Disposition = '20',

    [Parameter(ParameterSetName='LastDays', HelpMessage='Count items with NotBefore >= (Now - LastDays).')]
    [ValidateRange(1, 36500)]
    [int]$LastDays,

    [Parameter(ParameterSetName='Range', HelpMessage='Start of validity (NotBefore >= From). String; leave empty to ignore.')]
    [AllowNull()]
    [string]$From,

    [Parameter(ParameterSetName='Range', HelpMessage='End of validity (NotAfter <= To). String; leave empty to ignore.')]
    [AllowNull()]
    [string]$To,

    [Parameter(HelpMessage='Print computed certutil command and exit (no execution).')]
    [switch]$Preview
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

function Parse-DateSafe {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $null }
    $styles = [System.Globalization.DateTimeStyles]::AssumeLocal
    $cultures = @(
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.CultureInfo]::CurrentCulture
    )
    foreach ($c in $cultures) {
        try {
            $dt = [DateTime]::Parse($Value, $c, $styles)
            return $dt
        } catch {}
    }
    throw "Could not parse date/time value '$Value'. Try 'yyyy-MM-dd HH:mm:ss'."
}

function New-RestrictString {
    param([string]$Disposition,[string]$From,[string]$To,[int]$LastDays)
    $parts = @()
    if ($Disposition -ne 'All') { $parts += "Disposition=$Disposition" }

    $fromDt = $null; $toDt = $null
    if ($PSBoundParameters.ContainsKey('LastDays') -and $LastDays) {
        $fromDt = (Get-Date).AddDays(-$LastDays)
    } else {
        if ($PSBoundParameters.ContainsKey('From')) { $fromDt = Parse-DateSafe -Value $From }
        if ($PSBoundParameters.ContainsKey('To'))   { $toDt   = Parse-DateSafe -Value $To }
        if ($fromDt -and $toDt -and $fromDt -gt $toDt) {
            throw "-From cannot be later than -To (From=$fromDt, To=$toDt)."
        }
    }

    if ($fromDt) { $parts += 'NotBefore>=' + $fromDt.ToString('yyyy-MM-dd HH:mm:ss') }
    if ($toDt)   { $parts += 'NotAfter<='  + $toDt.ToString('yyyy-MM-dd HH:mm:ss')  }

    if ($parts.Count -eq 0) { return $null } else { return ($parts -join ' AND ') }
}

function Invoke-Certutil {
    param([string[]]$ArgumentList,[int]$TimeoutSec=180)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'certutil.exe'
    $psi.Arguments = ($ArgumentList | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() }) -join ' '
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    Write-Verbose "Running: certutil.exe $($psi.Arguments)"
    if ($PSBoundParameters.ContainsKey('Preview') -and $Preview) {
        Write-Host "[Preview] certutil.exe $($psi.Arguments)"
        return ""
    }
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
    $restrict = New-RestrictString -Disposition $Disposition -From $From -To $To -LastDays $LastDays

    $args = @()
    if ($cfg) { $args += @('-config', ('"'+$cfg+'"')) }
    $args += @('-view')
    if ($restrict) { $args += @('-restrict', ('"'+$restrict+'"')) }
    $args += @('-out', 'RequestID')

    $out = Invoke-Certutil -ArgumentList $args -TimeoutSec 300

    # Count lines that contain "Request ID:" (language-agnostic regex)
    $count = 0
    foreach ($line in ($out -split "`r?`n")) {
        if ($line -match 'Request\s*ID\s*:') { $count++ }
    }

    $row = [pscustomobject]@{
        Timestamp   = (Get-Date).ToString('s')
        CAConfig    = $cfg
        Disposition = $Disposition
        From        = if ($PSBoundParameters.ContainsKey('LastDays')) { (Get-Date).AddDays(-$LastDays).ToString('s') } elseif ($From) { (Parse-DateSafe $From).ToString('s') } else { '' }
        To          = if ($To) { (Parse-DateSafe $To).ToString('s') } else { '' }
        Count       = $count
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $row | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote count= $count to $OutCsv"
}
catch {
    $err = [pscustomobject]@{ Timestamp=(Get-Date).ToString('s'); Error=$_.Exception.Message }
    try {
        if ($OutCsv) {
            New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
            $err | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
        }
    } catch {}
    Write-Error $_.Exception.Message
    exit 1
}
