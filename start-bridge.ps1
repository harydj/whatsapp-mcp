$binDir = "C:\Users\Hariyanto\AppData\Local\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin"
$env:Path = "$binDir;$env:Path"
$env:CGO_ENABLED = "1"
Set-Location "C:\Users\Hariyanto\plugins\whatsapp-mcp\whatsapp-bridge"
Write-Host "Starting WhatsApp bridge... Scan QR di WhatsApp jika muncul."
.\whatsapp-bridge.exe
