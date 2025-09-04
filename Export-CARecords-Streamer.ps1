<#
.SYNOPSIS
  Crash-safe exporter for Microsoft CA: no .NET event handlers, no ISE hangs.
  Streams certutil output via TEMP FILES and flushes every N rows to CSV.

.VERSION
  v1.3 – Stability-first on WMF 5.1 / Server 2012 R2:
        * No DataReceived event handlers (avoids ScriptBlock.GetContextFromTLS).
        * Uses Start-Process with -RedirectStandardOutput/-RedirectStandardError to files.
        * Parses the output file line-by-line and flushes every N rows.
        * RequestID window batching supported (no dates required).

.DESCRIPTION
  - Runs 'certutil -view' in a child process, redirecting stdout/stderr to files.
  - After the child exits (or hits timeout), parses the stdout file sequentially.
  - Writes CSV in chunks (FlushEvery) to keep memory flat even for huge exports.
  - Optional RequestID batching to keep each certutil pass bounded.

.PARAMETER OutCsv
  Path to final CSV. The directory is created if needed. Appends after first chunk.

.PARAMETER CAConfig
  Optional CA config "HOST\CA Common Name". If omitted, tries to auto-detect local CA instance.

.PARAMETER Disposition
  Filter by code or "All". Default 20 (Issued). Common: 20,21,9,22.

.PARAMETER Properties
  Friendly property list to export. Defaults cover typical inventory.

.PARAMETER FlushEvery
  How many records to buffer before writing to CSV (default 1000).

.PARAMETER TimeoutSec
  Timeout for each certutil run (default 0 = no timeout).

.PARAMETER RequestIDStart / RequestIDEnd / RequestIDBatchSize
  Optional RequestID windowing. Use when you want to avoid very long single certutil runs.

.PARAMETER ScratchDir
  Optional folder for temp stdout/stderr files. Defaults to $env:TEMP.

.PARAMETER Preview
  Print the certutil command(s) that WOULD run and exit.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateNotNullOrEmpty()]
    [string]$OutCsv,

    [Parameter()]
    [string]$CAConfig,

    [Parameter()]
    [ValidatePattern('^(All|[0-9]+)$')]
    [string]$Disposition = '20',

    [Parameter()]
    [string[]]$Properties = @(
        'RequestID','SerialNumber','CommonName','RequesterName',
        'CertificateTemplate','NotBefore','NotAfter','Thumbprint','UPN','SAN','SubjectDN'
    ),

    [Parameter()]
    [ValidateRange(1, 100000)]
    [int]$FlushEvery = 1000,

    [Parameter()]
    [ValidateRange(0, 86400)]
    [int]$TimeoutSec = 0,

    [Parameter()]
    [ValidateRange(1, 2147483647)]
    [int]$RequestIDStart,

    [Parameter()]
    [ValidateRange(1, 2147483647)]
    [int]$RequestIDEnd,

    [Parameter()]
    [ValidateRange(1000, 10000000)]
    [int]$RequestIDBatchSize = 50000,

    [Parameter()]
    [string]$ScratchDir = $env:TEMP,

    [Parameter()]
    [switch]$Preview
)

function Test-CertutilPresent {
    if (-not (Get-Command certutil.exe -ErrorAction SilentlyContinue)) {
        throw "certutil.exe not found. Install AD CS tools or run on the CA host."
    }
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

# Friendly -> certutil field mapping (OrderedDictionary)
$FieldMap = [ordered]@{
    'RequestID'           = 'RequestID'
    'SerialNumber'        = 'SerialNumber'
    'NotBefore'           = 'Certificate Effective Date'
    'NotAfter'            = 'Certificate Expiration Date'
    'RequesterName'       = 'Requester Name'
    'CommonName'          = 'Common Name'
    'SubjectDN'           = 'Distinguished Name'
    'CertificateTemplate' = 'Certificate Template'
    'Thumbprint'          = 'Certificate Hash'
    'UPN'                 = 'UPN'
    'SAN'                 = 'SAN'
}

function Ensure-Dir([string]$path) {
    $dir = Split-Path -Parent $path
    if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
}

function New-Restrict {
    param(
        [string]$Disposition,
        [Nullable[int]]$ReqStart,
        [Nullable[int]]$ReqEnd
    )
    $parts = @()
    if ($Disposition -ne 'All') { $parts += "Disposition=$Disposition" }
    if ($ReqStart) { $parts += "RequestID>=$ReqStart" }
    if ($ReqEnd)   { $parts += "RequestID<=$ReqEnd" }
    if ($parts.Count -eq 0) { return $null } else { return ($parts -join ' AND ') }
}

function Build-CertutilArgs {
    param([string]$CAConfig,[string]$Restrict,[string[]]$OutFields)
    $args = @()
    if ($CAConfig) { $args += @('-config', ('"'+$CAConfig+'"')) }
    $args += @('-view')
    if ($Restrict) { $args += @('-restrict', ('"'+$Restrict+'"')) }
    $args += @('-out', ('"'+($OutFields -join ',')+'"'))
    return $args
}

function Start-CertutilToFiles {
    param([string[]]$Args,[string]$ScratchDir,[int]$TimeoutSec,[switch]$Preview)

    if (-not (Test-Path $ScratchDir)) { New-Item -ItemType Directory -Path $ScratchDir -Force | Out-Null }
    $guid = [Guid]::NewGuid().ToString('N')
    $stdout = Join-Path $ScratchDir "certutil_$guid.out.txt"
    $stderr = Join-Path $ScratchDir "certutil_$guid.err.txt"

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'cmd.exe'
    # Use cmd.exe /c to have reliable redirection on downlevel hosts
    $cmd = 'certutil.exe ' + (($Args | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() }) -join ' ')
    $psi.Arguments = '/c ' + $cmd + ' 1>"' + $stdout + '" 2>"' + $stderr + '"'
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    Write-Verbose "cmd.exe $($psi.Arguments)"
    if ($Preview) {
        Write-Host "[Preview] $cmd"
        return @('PREVIEW',$stdout,$stderr)
    }

    $proc = [System.Diagnostics.Process]::Start($psi)

    if ($TimeoutSec -gt 0) {
        if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
            try { $proc.Kill() } catch {}
            throw "certutil timed out after $TimeoutSec seconds."
        }
    } else {
        $proc.WaitForExit()
    }

    $exit = $proc.ExitCode
    return @($exit,$stdout,$stderr)
}

function Parse-StdoutFile {
    param(
        [string]$StdoutPath,
        [string[]]$Properties,
        [hashtable]$FieldMap,
        [int]$FlushEvery,
        [string]$OutCsv,
        [switch]$Append
    )

    $inverse = @{}
    foreach ($p in $Properties) {
        $f = $FieldMap[$p]
        if ($f) { $inverse[$f] = $p }
    }

    $headerWritten = [ref]$false
    if ($Append) { $headerWritten = [ref]$true }

    $current = [ordered]@{}
    $chunk = New-Object System.Collections.Generic.List[object]
    $rows = 0

    $fs = [System.IO.File]::Open($StdoutPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $sr = New-Object System.IO.StreamReader($fs, [Text.Encoding]::Default)
        try {
            while (-not $sr.EndOfStream) {
                $line = $sr.ReadLine()
                if ($null -eq $line) { continue }

                if ($line -match '^certutil:' -or $line -match '^-{5,}$' -or $line -match '^\s*$') {
                    if ($current.Count) {
                        $o = [ordered]@{}
                        foreach ($p in $Properties) {
                            $name = $FieldMap[$p]
                            $val  = $current[$name]
                            if ($p -eq 'CertificateTemplate' -and $val) {
                                if ($val -match '^(?<name>[^\(]+)\s*\(') { $val = $Matches.name.Trim() }
                            }
                            if ($p -eq 'Thumbprint' -and $val) { $val = ($val -replace '\s','').ToUpper() }
                            $o[$p] = $val
                        }
                        $chunk.Add([pscustomobject]$o)
                        $rows++
                        $current = [ordered]@{}

                        if ($chunk.Count -ge $FlushEvery) {
                            if (-not $headerWritten.Value) {
                                $chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
                                $headerWritten.Value = $true
                            } else {
                                $chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8 -Append
                            }
                            $chunk.Clear()
                        }
                    }
                    continue
                }

                $i = $line.IndexOf(':')
                if ($i -gt 0) {
                    $k = ($line.Substring(0,$i)).Trim()
                    $v = ($line.Substring($i+1)).Trim()
                    if ($inverse.ContainsKey($k)) {
                        if ($current.Contains($k)) {
                            $current[$k] = ($current[$k] + " " + $v).Trim()
                        } else {
                            $current[$k] = $v
                        }
                    }
                } else {
                    if ($current.Count) {
                        $lastKey = ($current.Keys | Select-Object -Last 1)
                        if ($lastKey) { $current[$lastKey] = ($current[$lastKey] + " " + $line.Trim()).Trim() }
                    }
                }
            }
        } finally {
            $sr.Close()
        }
    } finally {
        $fs.Close()
    }

    if ($current.Count) {
        $o = [ordered]@{}
        foreach ($p in $Properties) {
            $name = $FieldMap[$p]
            $val  = $current[$name]
            if ($p -eq 'CertificateTemplate' -and $val) {
                if ($val -match '^(?<name>[^\(]+)\s*\(') { $val = $Matches.name.Trim() }
            }
            if ($p -eq 'Thumbprint' -and $val) { $val = ($val -replace '\s','').ToUpper() }
            $o[$p] = $val
        }
        $chunk.Add([pscustomobject]$o)
        $rows++
    }

    if ($chunk.Count -gt 0) {
        if (-not $headerWritten.Value) {
            $chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
            $headerWritten.Value = $true
        } else {
            $chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8 -Append
        }
        $chunk.Clear()
    }

    return $rows
}

try {
    Test-CertutilPresent
    $cfg = Resolve-CAConfig -Explicit $CAConfig

    # Validate properties (membership test safe for OrderedDictionary)
    $unknown = $Properties | Where-Object { -not $FieldMap.Contains($_) }
    if ($unknown) {
        $valid = ($FieldMap.Keys -join ', ')
        throw "Unknown property(ies): $($unknown -join ', '). Valid: $valid"
    }
    $outFields = $Properties | ForEach-Object { $FieldMap[$_] } | Select-Object -Unique

    Ensure-Dir $OutCsv

    $total = 0
    $headerExists = Test-Path $OutCsv

    if ($PSBoundParameters.ContainsKey('RequestIDStart') -and $PSBoundParameters.ContainsKey('RequestIDEnd')) {
        $batchId = 0
        for ($s = $RequestIDStart; $s -le $RequestIDEnd; $s += $RequestIDBatchSize) {
            $e = [Math]::Min($s + $RequestIDBatchSize - 1, $RequestIDEnd)
            $batchId++
            $restrict = New-Restrict -Disposition $Disposition -ReqStart $s -ReqEnd $e
            $args = Build-CertutilArgs -CAConfig $cfg -Restrict $restrict -OutFields $outFields
            $exit,$stdout,$stderr = Start-CertutilToFiles -Args $args -ScratchDir $ScratchDir -TimeoutSec $TimeoutSec -Preview:$Preview
            if ($Preview) { continue }

            if ($exit -ne 0) {
                $errText = ''
                if (Test-Path $stderr) { $errText = (Get-Content -Path $stderr -ErrorAction SilentlyContinue | Out-String).Trim() }
                throw "Batch $batchId ($s..$e) failed with exit $exit. $errText"
            }

            $rows = Parse-StdoutFile -StdoutPath $stdout -Properties $Properties -FieldMap $FieldMap -FlushEvery $FlushEvery -OutCsv $OutCsv -Append:$headerExists
            $total += $rows
            $headerExists = $true

            # Cleanup temp files
            Remove-Item -Path $stdout,$stderr -ErrorAction SilentlyContinue
            Write-Verbose ("Batch {0} [{1}..{2}]: {3} rows (total {4})" -f $batchId,$s,$e,$rows,$total)
        }
    } else {
        $restrict = New-Restrict -Disposition $Disposition -ReqStart $null -ReqEnd $null
        $args = Build-CertutilArgs -CAConfig $cfg -Restrict $restrict -OutFields $outFields
        $exit,$stdout,$stderr = Start-CertutilToFiles -Args $args -ScratchDir $ScratchDir -TimeoutSec $TimeoutSec -Preview:$Preview
        if ($Preview) { return }

        if ($exit -ne 0) {
            $errText = ''
            if (Test-Path $stderr) { $errText = (Get-Content -Path $stderr -ErrorAction SilentlyContinue | Out-String).Trim() }
            throw "certutil failed with exit $exit. $errText"
        }

        $rows = Parse-StdoutFile -StdoutPath $stdout -Properties $Properties -FieldMap $FieldMap -FlushEvery $FlushEvery -OutCsv $OutCsv -Append:$headerExists
        $total = $rows

        Remove-Item -Path $stdout,$stderr -ErrorAction SilentlyContinue
        Write-Verbose ("Streamed export (single pass): {0} rows" -f $rows)
    }

    Write-Host "Export complete. Total rows: $total -> $OutCsv"
}
catch {
    Write-Error $_.Exception.Message
    try {
        Ensure-Dir $OutCsv
        [pscustomobject]@{ Timestamp=(Get-Date).ToString('s'); Error=$_.Exception.Message } |
            Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    } catch {}
    exit 1
}