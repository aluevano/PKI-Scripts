<#
.SYNOPSIS
  Export Microsoft CA certificate records to CSV in a memory-safe way (streaming + flush-every-N).

.VERSION
  v1.2 – OrderedDictionary-safe: removed ALL .ContainsKey() usages; use .Contains() consistently.
         Safer continuation handling; minor robustness improvements.

.DESCRIPTION
  - Streams certutil output and writes CSV every X rows.
  - No date filters required. Optional RequestID window batching.
  - Select properties to export via -Properties (friendly names).

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

function Start-CertutilProcess {
    param([string[]]$Args,[int]$TimeoutSec,[switch]$Preview)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'certutil.exe'
    $psi.Arguments = ($Args | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() }) -join ' '
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    Write-Verbose "certutil.exe $($psi.Arguments)"
    if ($Preview) {
        Write-Host "[Preview] certutil.exe $($psi.Arguments)"
        return $null
    }

    $proc = New-Object System.Diagnostics.Process
    $proc.StartInfo = $psi

    $global:__sbErr = New-Object System.Text.StringBuilder
    $errorHandler = [System.Diagnostics.DataReceivedEventHandler]{
        param($sender,$e)
        if ($null -ne $e.Data) { [void]$global:__sbErr.AppendLine($e.Data) }
    }

    $null = $proc.Start()
    $proc.add_ErrorDataReceived($errorHandler)
    $proc.BeginErrorReadLine()

    return $proc
}

function Write-Chunk {
    param([System.Collections.Generic.List[object]]$Chunk,[string]$OutCsv,[ref]$HeaderWritten)
    if ($Chunk.Count -gt 0) {
        if (-not $HeaderWritten.Value) {
            $Chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
            $HeaderWritten.Value = $true
        } else {
            $Chunk | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8 -Append
        }
        $Chunk.Clear()
    }
}

function Parse-And-Flush {
    param(
        [System.Diagnostics.Process]$Proc,
        [string[]]$OutFields,
        [string[]]$Properties,
        [hashtable]$FieldMap,
        [int]$FlushEvery,
        [string]$OutCsv,
        [switch]$Append
    )
    # certutil field -> friendly property
    $inverse = @{}
    foreach ($p in $Properties) {
        $f = $FieldMap[$p]
        if ($f) { $inverse[$f] = $p }
    }

    $current = [ordered]@{}
    $chunk = New-Object System.Collections.Generic.List[object]
    $rows  = 0
    $w = New-Object System.IO.StreamReader($Proc.StandardOutput.BaseStream, [Text.Encoding]::Default)

    $headerWritten = [ref]$false
    if ($Append) { $headerWritten = [ref]$true }

    while (-not $w.EndOfStream) {
        $line = $w.ReadLine()
        if ($null -eq $line) { continue }

        # boundaries between records
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
                    Write-Chunk -Chunk $chunk -OutCsv $OutCsv -HeaderWritten $headerWritten
                }
            }
            continue
        }

        # key: value
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
            # continuation for previous kept key (wrapped lines)
            if ($current.Count) {
                $lastKey = ($current.Keys | Select-Object -Last 1)
                if ($lastKey) { $current[$lastKey] = ($current[$lastKey] + " " + $line.Trim()).Trim() }
            }
        }
    }

    # final record flush
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

    # final chunk write
    Write-Chunk -Chunk $chunk -OutCsv $OutCsv -HeaderWritten $headerWritten

    return $rows
}

try {
    Test-CertutilPresent
    $cfg = Resolve-CAConfig -Explicit $CAConfig

    # Validate properties (OrderedDictionary-safe membership test)
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
            if ($Preview) { Write-Host "[Preview] Batch ${batchId}: $s..$e" }
            $proc = Start-CertutilProcess -Args $args -TimeoutSec $TimeoutSec -Preview:$Preview
            if ($Preview) { continue }

            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            $rows = Parse-And-Flush -Proc $proc -OutFields $outFields -Properties $Properties -FieldMap $FieldMap -FlushEvery $FlushEvery -OutCsv $OutCsv -Append:$headerExists

            if ($TimeoutSec -gt 0) {
                $deadline = (Get-Date).AddSeconds($TimeoutSec)
                while (-not $proc.HasExited) {
                    Start-Sleep -Milliseconds 200
                    if ((Get-Date) -gt $deadline) {
                        try { $proc.Kill() } catch {}
                        throw "certutil timed out after $TimeoutSec seconds."
                    }
                }
            } else {
                $proc.WaitForExit()
            }

            if ($proc.ExitCode -ne 0) {
                $err = $global:__sbErr.ToString().Trim()
                if (-not $err) { $err = "certutil exit code $($proc.ExitCode)" }
                throw "Batch $batchId ($s..$e) failed: $err"
            }

            $sw.Stop()
            $total += $rows
            $headerExists = $true
            Write-Verbose ("Batch {0} [{1}..{2}]: {3} rows in {4}s (total {5})" -f $batchId,$s,$e,$rows,[Math]::Round($sw.Elapsed.TotalSeconds,2),$total)
        }
    }
    else {
        # Single streaming export (no batching)
        $restrict = New-Restrict -Disposition $Disposition -ReqStart $null -ReqEnd $null
        $args = Build-CertutilArgs -CAConfig $cfg -Restrict $restrict -OutFields $outFields
        $proc = Start-CertutilProcess -Args $args -TimeoutSec $TimeoutSec -Preview:$Preview
        if ($Preview) { return }

        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $rows = Parse-And-Flush -Proc $proc -OutFields $outFields -Properties $Properties -FieldMap $FieldMap -FlushEvery $FlushEvery -OutCsv $OutCsv -Append:$headerExists

        if ($TimeoutSec -gt 0) {
            $deadline = (Get-Date).AddSeconds($TimeoutSec)
            while (-not $proc.HasExited) {
                Start-Sleep -Milliseconds 200
                if ((Get-Date) -gt $deadline) {
                    try { $proc.Kill() } catch {}
                    throw "certutil timed out after $TimeoutSec seconds."
                }
            }
        } else {
            $proc.WaitForExit()
        }

        if ($proc.ExitCode -ne 0) {
            $err = $global:__sbErr.ToString().Trim()
            if (-not $err) { $err = "certutil exit code $($proc.ExitCode)" }
            throw $err
        }

        $sw.Stop()
        $total = $rows
        Write-Verbose ("Streamed export: {0} rows in {1}s" -f $rows, [Math]::Round($sw.Elapsed.TotalSeconds,2))
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