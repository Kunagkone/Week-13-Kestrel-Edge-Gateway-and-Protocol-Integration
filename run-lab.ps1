param (
    [Parameter(Position = 0)]
    [ValidateSet("1", "2", "3", "13-1", "13-2", "13-3")]
    [string]$Lab = "3"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  Kestrel Converged Edge Gateway - Lab Runner" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan

switch ($Lab) {
    { $_ -in "1", "13-1" } {
        Write-Host ">> กำลังรัน Lab 13-1: Embedded MQTT Broker (Port 5000, 1883)..." -ForegroundColor Green
        Set-Location "$PSScriptRoot\Example_Codes\ConvergedEdgeGateway"
        dotnet run
    }
    { $_ -in "2", "13-2" } {
        Write-Host ">> กำลังรัน Lab 13-2: Hardware Serial to MQTT & REST (พร้อม ESP32 Simulator Mode)..." -ForegroundColor Green
        Write-Host ">> REST API: http://localhost:5000/api/telemetry/latest" -ForegroundColor DarkCyan
        Set-Location "$PSScriptRoot\Example_Codes\ConvergedEdgeGateway_Lab2"
        dotnet run
    }
    { $_ -in "3", "13-3" } {
        Write-Host ">> กำลังรัน Lab 13-3: Realtime SignalR Hub + Web Dashboard Glassmorphism (พร้อม Simulator Mode)..." -ForegroundColor Green
        Write-Host ">> เปิดหน้าเว็บแดชบอร์ดได้ที่: http://localhost:5000" -ForegroundColor Yellow
        Set-Location "$PSScriptRoot\Example_Codes\ConvergedEdgeGateway_Lab3"
        dotnet run
    }
}
