[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$OutCsv
)

function Parse-Urls([string]$type,[string]$text){
    $rows = @()
    foreach($line in ($text -split "`r?`n")){
        if ($line -match '^\s*(\d+):\s*(\S+)\s*(?:\((.+)\))?\s*$'){
            $rows += [pscustomobject]@{
                Type      = $type
                Seq       = [int]$Matches[1]
                Url       = $Matches[2]
                Flags     = $Matches[3]
            }
        }
    }
    if (-not $rows){ $rows = @([pscustomobject]@{Type=$type; Seq=''; Url=''; Flags=''}) }
    return $rows
}

try {
    $crl = & certutil.exe -getreg CA\CRLPublicationURLs 2>&1
    $aia = & certutil.exe -getreg CA\CACertPublicationURLs 2>&1

    $rows = @()
    $rows += Parse-Urls -type 'CRL' -text $crl
    $rows += Parse-Urls -type 'AIA' -text $aia

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutCsv) | Out-Null
    $rows | Sort-Object Type, Seq | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
    Write-Host "Wrote CRL/AIA URLs to $OutCsv"
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
