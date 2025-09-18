<# 
.SYNOPSIS
  PowerShell 5.1-safe exporter: streams 'certutil -view -csv' to rolling CSV chunks.
  Handles CAConfig with spaces and extracts only the Template *name*.

.PARAMS
  -CAConfig         e.g. 'CA01\Contoso Issuing CA'  (spaces OK)
  -OutputDirectory  e.g. 'D:\CA-Export'
  -BaseFileName     base for chunk files (default: CA_DB_Export)
  -BatchSize        rows per chunk (default: 100000)
  -Restrict         certutil -restrict filter (optional; spaces OK)
  -Columns          Comma-separated list of fields to request from certutil
  -TemplateField    Header name of the template column if not "CertificateTemplate"
  -CertutilPath     Path to certutil (default: certutil)
#>

param(
  [Parameter(Mandatory=$true)]
  [string]$CAConfig,

  [Parameter(Mandatory=$true)]
  [string]$OutputDirectory,

  [string]$BaseFileName = "CA_DB_Export",
  [int]$BatchSize = 100000,
  [string]$Restrict = "",
  # Keep list lean; include the template column you need.
  [string]$Columns = "RequestID,SerialNumber,NotBefore,NotAfter,RequesterName,CertificateTemplate,CertificateHash,RequestDisposition",
  # If your header isn’t exactly "CertificateTemplate", set this (e.g., "Certificate Template")
  [string]$TemplateField = "",
  [string]$CertutilPath = "certutil"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- Ensure output directory
if (-not (Test-Path -LiteralPath $OutputDirectory)) {
  New-Item -ItemType Directory -Path $OutputDirectory | Out-Null
}

# --- Prep columns
$colList = $Columns.Split(',').ForEach({ $_.Trim() }) | Where-Object { $_ -ne "" }
if ($colList.Count -eq 0) { throw "No columns specified." }

# --- PS 5.1-safe quoting helper
function Quote-Arg([string]$s) {
  if ($null -eq $s) { return '""' }
  return '"' + ($s -replace '"','""') + '"'
}

# --- Build certutil command line (single string of args for PS 5.1)
$columnsArgQuoted  = Quote-Arg ($colList -join ',')
$configQuoted      = Quote-Arg $CAConfig
$restrictSegment   = ""
if (-not [string]::IsNullOrWhiteSpace($Restrict)) {
  $restrictSegment = " -restrict " + (Quote-Arg $Restrict)
}
$allArgs = @(
  "-config $configQuoted",
  "-view",
  "-out $columnsArgQuoted",
  "$restrictSegment",
  "csv"
) -join ' '

#$allArgs += $restrictSegment 

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
$psi.Arguments = $allArgs
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
$script:totalRows     = 0L
$script:rowsInChunk   = 0
$script:chunkIndex    = 1
$script:startTime     = Get-Date
$script:lastTickTime  = $script:startTime
$script:lastTickRows  = 0L
[int]$statusEvery     = 10000

# ----- CSV helpers -----

function Split-CsvLine {
  <# Robust single-line CSV splitter honoring quotes and doubled quotes #>
  param([Parameter(Mandatory=$true)][string]$Line)

  # Drop UTF-8 BOM if present on first line
  if ($Line.Length -ge 1 -and $Line[0] -eq [char]0xFEFF) {
    $Line = $Line.TrimStart([char]0xFEFF)
  }

  $inQuotes = $false
  $sb = New-Object System.Text.StringBuilder
  $fields = New-Object System.Collections.Generic.List[string]
  for ($i=0; $i -lt $Line.Length; $i++) {
    $ch = $Line[$i]
    if ($ch -eq '"') {
      if ($inQuotes -and ($i + 1 -lt $Line.Length) -and $Line[$i+1] -eq '"') {
        [void]$sb.Append('"'); $i++
      } else {
        $inQuotes = -not $inQuotes
      }
    } elseif ($ch -eq ',' -and -not $inQuotes) {
      $fields.Add($sb.ToString()); $sb.Clear() | Out-Null
    } else {
      [void]$sb.Append($ch)
    }
  }
  $fields.Add($sb.ToString())
  return ,$fields.ToArray()
}

# Extract the Certificate Template *name* from common certutil formats
function Get-TemplateNameOnly {
  param([string]$Raw)

  if ([string]::IsNullOrWhiteSpace($Raw)) { return "" }
  $val = $Raw.Trim()

  # (OID) Name  -> prefer text inside last parentheses
  $parenMatch = [regex]::Match($val, '\(([^)]*)\)\s*$')
  if ($parenMatch.Success -and $parenMatch.Groups[1].Value.Trim()) {
    return $parenMatch.Groups[1].Value.Trim()
  }

  # OID plus name  e.g. 1.3.6.... WebServer1W   (quotes around OID are ok)
  $m = [regex]::Match($val, '^\s*"?\d+(?:\.\d+)+"?\s+(?<name>.+?)\s*$')
  if ($m.Success) { return $m.Groups['name'].Value.Trim() }

  # Only an OID? then empty
  if ($val -match '^\s*"?\d+(?:\.\d+)+"?\s*$') { return "" }

  # Already a name (strip surrounding quotes)
  return $val.Trim('"')
}

function Escape-CsvField {
  param([string]$s)
  if ($null -eq $s) { return "" }
  '"' + ($s -replace '"','""') + '"'
}

# Prepare first chunk writer (keep same header ordering as certutil)
function New-ChunkWriter {
  param([int]$Index, [string[]]$HeaderCols)

  $file = Join-Path $OutputDirectory ("{0}_chunk{1:000000}.csv" -f $BaseFileName, $Index)
  $sw = New-Object System.IO.StreamWriter($file, $false, [System.Text.Encoding]::UTF8)
  $sw.WriteLine(($HeaderCols -join ','))
  return @{ Writer = $sw; Path = $file }
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

# ---- Read header (first non-empty line) ----
[string]$headerLine = $null
while (-not $stdOut.EndOfStream) {
  $headerLine = $stdOut.ReadLine()
  if (-not [string]::IsNullOrWhiteSpace($headerLine)) { break }
}
if ([string]::IsNullOrWhiteSpace($headerLine)) { throw "certutil produced no output." }

# Parse header into an array
$headerCols = Split-CsvLine -Line $headerLine | ForEach-Object { $_.Trim() }

# Auto-detect template field if not provided
if ([string]::IsNullOrWhiteSpace($TemplateField)) {
  $TemplateField = ($headerCols | Where-Object {
    $_ -match 'CertificateTemplate' -or $_ -match 'Certificate Template'
  } | Select-Object -First 1)
  if (-not $TemplateField) { $TemplateField = "CertificateTemplate" }
}

# Open first chunk
$script:chunk = New-ChunkWriter -Index $script:chunkIndex -HeaderCols $headerCols

# ---- Stream data lines ----
while (-not $stdOut.EndOfStream) {
  $line = $stdOut.ReadLine()
  if ([string]::IsNullOrWhiteSpace($line)) { continue }

  # Convert a single CSV line into a PSCustomObject based on header
  $obj = ConvertFrom-Csv -InputObject $line -Header $headerCols

  # Normalize the template column to ONLY the display name
  if ($obj.PSObject.Properties.Name -contains $TemplateField) {
    $obj.$TemplateField = Get-TemplateNameOnly -Raw ([string]$obj.$TemplateField)
  }

  # Write out fields in original header order
  $values = foreach ($h in $headerCols) {
    Escape-CsvField ([string]$obj.$h)
  }
  $script:chunk.Writer.WriteLine(($values -join ','))

  # Counters & rollover
  $script:rowsInChunk++; $script:totalRows++
  if ($script:rowsInChunk -ge $BatchSize) {
    $script:chunk.Writer.Flush(); $script:chunk.Writer.Close()
    Write-Host ("Chunk complete: {0} rows -> {1}" -f $script:rowsInChunk, $script:chunk.Path) -ForegroundColor Green
    $script:rowsInChunk = 0
    $script:chunkIndex++
    $script:chunk = New-ChunkWriter -Index $script:chunkIndex -HeaderCols $headerCols
  }

  if (($script:totalRows -gt 0) -and (($script:totalRows % $statusEvery) -eq 0)) { Print-Status }
}

# ---- Finalize ----
if ($script:chunk.Writer) { $script:chunk.Writer.Flush(); $script:chunk.Writer.Close() }

$errText = $stdErr.ReadToEnd()
$proc.WaitForExit()

Write-Host ""
Write-Host ("Done. Total rows: {0:n0}, Chunks: {1}, Duration: {2:hh\:mm\:ss}" -f $script:totalRows, $script:chunkIndex, ((Get-Date) - $script:startTime)) -ForegroundColor Cyan
if ($errText) {
  Write-Warning "certutil reported messages on stderr:"
  Write-Host $errText
}
