<# 
.SYNOPSIS
  Streams the CA DB via 'certutil -view -csv' safely to chunked CSV files.
  Fixes row parsing for quoted CSV and extracts only the Certificate Template *name*.

.PARAMS
  -CAConfig         e.g. 'CAHOST\Contoso-CA'
  -OutputDirectory  e.g. 'D:\CA-Export'
  -BaseFileName     base for chunk files (default: CA_DB_Export)
  -BatchSize        rows per chunk (default: 100000)
  -Restrict         certutil -restrict filter (optional)
  -Columns          Comma-separated list of fields to request from certutil
  -TemplateField    The header name that holds the certificate template info (default tries to auto-detect)
  -CertutilPath     Path to certutil (default: certutil)

.NOTES
  - Requires read access to the CA DB.
  - Does *not* hold the entire dataset in memory.
#>

param(
  [Parameter(Mandatory=$true)]
  [string]$CAConfig,

  [Parameter(Mandatory=$true)]
  [string]$OutputDirectory,

  [string]$BaseFileName = "CA_DB_Export",
  [int]$BatchSize = 100000,
  [string]$Restrict = "",
  # Keep your list tight for performance; include the template field.
  [string]$Columns = "RequestID,SerialNumber,NotBefore,NotAfter,RequesterName,CertificateTemplate,CertificateHash,RequestDisposition",
  # If your header isn’t exactly "CertificateTemplate", set this (e.g., "Certificate Template")
  [string]$TemplateField = "",
  [string]$CertutilPath = "certutil"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- Prep output directory
if (-not (Test-Path -LiteralPath $OutputDirectory)) {
  New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
}

# Normalize columns -> array and trim
$colList = $Columns.Split(',').ForEach({ $_.Trim() }) | Where-Object { $_ -ne "" }
if ($colList.Count -eq 0) { throw "No columns specified." }

# Build certutil args (CSV mode)
$restrictArg = if ([string]::IsNullOrWhiteSpace($Restrict)) { "" } else { "-restrict `"$Restrict`"" }
$columnsArg  = "-out `"$($colList -join ',')`""

$certutilArgs = @(
  "-config", $CAConfig,
  "-view",
  $columnsArg,
  "csv"
)
if ($restrictArg) { $certutilArgs += $restrictArg }

Write-Host "Launching certutil (CSV mode)..." -ForegroundColor Cyan
Write-Host "  CA Config : $CAConfig"
Write-Host "  Restrict  : $Restrict"
Write-Host "  Columns   : $Columns"
Write-Host "  BatchSize : $BatchSize"
Write-Host "  OutputDir : $OutputDirectory"
Write-Host ""

# --- Start certutil process
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $CertutilPath
$psi.Arguments = ($certutilArgs -join ' ')
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError  = $true
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true

$proc = New-Object System.Diagnostics.Process
$proc.StartInfo = $psi
if (-not $proc.Start()) { throw "Failed to start certutil." }

$stdOut = $proc.StandardOutput
$stdErr = $proc.StandardError

# ---------- Shared state ----------
$script:totalRows   = 0L
$script:rowsInChunk = 0
$script:chunkIndex  = 1
$script:startTime   = Get-Date
$script:lastTickTime = $script:startTime
$script:lastTickRows = 0L
[int]$statusEvery = 10000

# ----- CSV helpers -----

function Split-CsvLine {
  <#
    Robustly split a single CSV line into fields (RFC-style quotes).
    Returns [string[]] fields.
  #>
  param([Parameter(Mandatory=$true)][string]$Line)

  $inQuotes = $false
  $sb = New-Object System.Text.StringBuilder
  $fields = New-Object System.Collections.Generic.List[string]
  for ($i=0; $i -lt $Line.Length; $i++) {
    $ch = $Line[$i]
    if ($ch -eq '"') {
      if ($inQuotes -and ($i + 1 -lt $Line.Length) -and $Line[$i+1] -eq '"') {
        # Escaped quote -> append one quote and skip the next
        $null = $sb.Append('"'); $i++
      } else {
        $inQuotes = -not $inQuotes
      }
    } elseif ($ch -eq ',' -and -not $inQuotes) {
      $fields.Add($sb.ToString()); $sb.Clear() | Out-Null
    } else {
      $null = $sb.Append($ch)
    }
  }
  $fields.Add($sb.ToString())
  return ,$fields.ToArray()
}

# Extract only the Template *Name* from various certutil formats.
function Get-TemplateNameOnly {
  param([string]$Raw)

  if ([string]::IsNullOrWhiteSpace($Raw)) { return "" }

  $val = $Raw.Trim()

  # Cases seen:
  # 1) 1.3.6.1.4.1.311.... WebServer1W
  # 2) "1.3.6.1.4.1.311...." WebServer1W
  # 3) 1.3.6.1.4.1.311.... (WebServer1W)
  # 4) WebServer1W
  # 5) pKIExtendedKeyUsage / weird spacing or extra quotes

  # If parentheses exist, prefer what's inside the last pair.
  $parenMatch = [regex]::Match($val, '\(([^)]*)\)\s*$')
  if ($parenMatch.Success -and $parenMatch.Groups[1].Value.Trim()) {
    return $parenMatch.Groups[1].Value.Trim()
  }

  # If there's an OID followed by a name, capture the trailing name.
  $m = [regex]::Match($val, '^\s*"?\d+(?:\.\d+)+"?\s+(?<name>.+?)\s*$')
  if ($m.Success) {
    return $m.Groups['name'].Value.Trim()
  }

  # If it looks like only an OID (no name), return empty string
  if ($val -match '^\s*"?\d+(?:\.\d+)+"?\s*$') { return "" }

  # Otherwise, assume it's already a name.
  return $val.Trim('"')
}

# Discover the header (first non-empty line)
[string]$headerLine = $null
while (-not $stdOut.EndOfStream) {
  $headerLine = $stdOut.ReadLine()
  if (-not [string]::IsNullOrWhiteSpace($headerLine)) { break }
}
if ([string]::IsNullOrWhiteSpace($headerLine)) {
  throw "certutil produced no output."
}

# Parse header into an array of column names
$headerCols = Split-CsvLine -Line $headerLine | ForEach-Object { $_.Trim() }

# If user didn’t specify TemplateField, try to locate it.
if ([string]::IsNullOrWhiteSpace($TemplateField)) {
  $TemplateField = ($headerCols | Where-Object {
    $_ -match 'CertificateTemplate' -or $_ -match 'Certificate Template'
  } | Select-Object -First 1)
  if (-not $TemplateField) {
    # Fall back to a common field name, but don’t fail hard
    $TemplateField = "CertificateTemplate"
  }
}

# Prepare writer for the first chunk
function New-ChunkWriter {
  param([int]$Index)

  $file = Join-Path $OutputDirectory ("{0}_chunk{1:000000}.csv" -f $BaseFileName, $Index)
  $sw = New-Object System.IO.StreamWriter($file, $false, [System.Text.Encoding]::UTF8)

  # We will write the *same* headers that we read, but we’ll normalize the template field
  # to contain only the template display name.
  $sw.WriteLine(($headerCols -join ','))
  return @{ Writer = $sw; Path = $file }
}
$script:chunk = New-ChunkWriter -Index $script:chunkIndex

function Escape-CsvField {
  param([string]$s)
  if ($null -eq $s) { return "" }
  '"' + ($s -replace '"','""') + '"'
}

function Print-Status {
  $now = Get-Date
  $elapsed = $now - $script:startTime
  $deltaT = $now - $script:lastTickTime
  $deltaRows = $script:totalRows - $script:lastTickRows
  $rps = if ($deltaT.TotalSeconds -gt 0) { [math]::Round($deltaRows / $deltaT.TotalSeconds, 2) } else { 0 }
  Write-Host ("[{0}] Rows: {1:n0} | Chunk: {2} ({3:n0}/{4:n0}) | Rate: {5} rows/s | Elapsed: {6:hh\:mm\:ss}" -f (Get-Date), $script:totalRows, $script:chunkIndex, $script:rowsInChunk, $BatchSize, $rps, $elapsed)
  $script:lastTickTime = $now
  $script:lastTickRows = $script:totalRows
}

# ---- Stream data lines ----
while (-not $stdOut.EndOfStream) {
  $line = $stdOut.ReadLine()
  if ([string]::IsNullOrWhiteSpace($line)) { continue }

  # Convert one row line into an object using our known header
  $obj = ConvertFrom-Csv -InputObject $line -Header $headerCols

  # Normalize the template field (if present)
  if ($obj.PSObject.Properties.Name -contains $TemplateField) {
    $obj.$TemplateField = Get-TemplateNameOnly -Raw ([string]$obj.$TemplateField)
  }

  # Write out in the original header order
  $values = foreach ($h in $headerCols) {
    Escape-CsvField ([string]$obj.$h)
  }
  $script:chunk.Writer.WriteLine(($values -join ','))

  # Counters and rollover
  $script:rowsInChunk++; $script:totalRows++
  if ($script:rowsInChunk -ge $BatchSize) {
    $script:chunk.Writer.Flush(); $script:chunk.Writer.Close()
    Write-Host ("Chunk complete: {0} rows -> {1}" -f $script:rowsInChunk, $script:chunk.Path) -ForegroundColor Green
    $script:rowsInChunk = 0
    $script:chunkIndex++
    $script:chunk = New-ChunkWriter -Index $script:chunkIndex
  }

  if (($script:totalRows -gt 0) -and (($script:totalRows % $statusEvery) -eq 0)) {
    Print-Status
  }
}

# Finalize
if ($script:chunk.Writer) { $script:chunk.Writer.Flush(); $script:chunk.Writer.Close() }

$errText = $stdErr.ReadToEnd()
$proc.WaitForExit()

Write-Host ""
Write-Host ("Done. Total rows: {0:n0}, Chunks: {1}, Duration: {2:hh\:mm\:ss}" -f $script:totalRows, $script:chunkIndex, ((Get-Date) - $script:startTime)) -ForegroundColor Cyan
if ($errText) {
  Write-Warning "certutil reported messages on stderr:"
  Write-Host $errText
}
