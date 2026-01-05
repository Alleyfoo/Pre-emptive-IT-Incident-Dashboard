# Emits a schema-compliant snapshot.json for the last 24h (System + Application).
# Usage:
#   ./snapshot.ps1 -HostId HOST-123 -OutputPath ./snapshot.json -HoursBack 24
# Schedule via Task Scheduler to drop files into a folder watched for upload to GCS.

param(
    [Parameter(Mandatory = $true)][string]$HostId,
    [Parameter(Mandatory = $true)][string]$OutputPath,
    [int]$HoursBack = 24
)

$start = (Get-Date).AddHours(-1 * $HoursBack)
$end = Get-Date

function Convert-Level {
    param([int]$Level)
    switch ($Level) {
        1 { return "Critical" }
        2 { return "Error" }
        3 { return "Warning" }
        4 { return "Information" }
        Default { return "Information" }
    }
}

function Sanitize-Message {
    param([string]$Message)
    if (-not $Message) { return "" }
    $msg = $Message
    $msg = $msg -replace "[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[REDACTED_EMAIL]"
    $msg = $msg -replace "\\\\[A-Za-z0-9_.-]+\\[^\s]+", "[REDACTED_PATH]"
    $msg = $msg -replace "[A-Za-z]:\\[^\s]+", "[REDACTED_PATH]"
    $msg = $msg -replace "\b(\d{1,3}\.\d{1,3}\.\d{1,3})\.\d{1,3}\b", '$1.0/24'
    return $msg.Substring(0, [Math]::Min($msg.Length, 4000))
}

$filters = @(
    @{ LogName = "System"; StartTime = $start; EndTime = $end },
    @{ LogName = "Application"; StartTime = $start; EndTime = $end }
)

$events = @()
foreach ($filter in $filters) {
    try {
        $evts = Get-WinEvent -FilterHashtable $filter -ErrorAction Stop
        foreach ($evt in $evts) {
            $events += [PSCustomObject]@{
                ts        = $evt.TimeCreated.ToString("o")
                level     = Convert-Level -Level $evt.Level
                source    = "WindowsEventLog:$($evt.LogName)"
                channel   = $evt.LogName
                provider  = $evt.ProviderName
                event_id  = $evt.Id
                record_id = $evt.RecordId
                message   = Sanitize-Message -Message $evt.Message
                data      = @{}
                tags      = @()
            }
        }
    } catch {
        Write-Warning "Failed to read $($filter.LogName): $_"
    }
}

$snapshot = [PSCustomObject]@{
    schema_version = "1.0"
    snapshot_id    = "$HostId-$($end.ToString('yyyyMMddHHmm'))"
    host_id        = $HostId
    user_id        = ""
    generated_at   = (Get-Date).ToString("o")
    window         = @{ start = $start.ToString("o"); end = $end.ToString("o") }
    device         = @{ hostname = $env:COMPUTERNAME }
    collector      = @{ name = "powershell_winlog"; version = "1.0"; method = "local_script" }
    filters        = @{ levels = @("Critical","Error","Warning","Information","Verbose"); providers_allowlist = @() }
    events         = $events
    stats          = @{
        event_count    = $events.Count
        critical_count = ($events | Where-Object { $_.level -eq "Critical" }).Count
        error_count    = ($events | Where-Object { $_.level -eq "Error" }).Count
        warning_count  = ($events | Where-Object { $_.level -eq "Warning" }).Count
    }
}

$json = $snapshot | ConvertTo-Json -Depth 6
Set-Content -Path $OutputPath -Value $json -Encoding UTF8
Write-Host "Snapshot written to $OutputPath with $($events.Count) events."
