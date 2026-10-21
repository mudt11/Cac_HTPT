@echo off
setlocal DisableDelayedExpansion
chcp 65001 > nul
echo ====================================================================
echo   BERKELEY TIME SYNC - MO TCP 9999 VA UDP 8888
echo ====================================================================
echo Chi quan ly hai rule co dinh danh rieng cua du an.
echo Khong thay doi rule co san, ke ca rule cu Berkeley Time Server.
echo.
set "PS_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"
if not exist "%PS_EXE%" (
    echo [LOI] Khong tim thay Windows PowerShell.
    pause
    exit /b 1
)
"%PS_EXE%" -NoLogo -NoProfile -NonInteractive -Command ^
 "$ErrorActionPreference = 'Stop';" ^
 "$owner = 'BerkeleyTimeSync managed ports v1; owner=82fa8ceb-2312-4aad-9144-f7ab438857f1';" ^
 "$group = 'BerkeleyTimeSync.Project';" ^
 "$specs = @(" ^
 "@{ Name='BerkeleyTimeSync.Project.TCP9999.82fa8ceb'; Protocol='TCP'; Port=9999 }," ^
 "@{ Name='BerkeleyTimeSync.Project.UDP8888.82fa8ceb'; Protocol='UDP'; Port=8888 }" ^
 ");" ^
 "function Test-ProjectRule($rule, $spec) {" ^
 "if ($rule.Name -ne $spec.Name -or $rule.Description -ne $owner -or $rule.Group -ne $group) { return $false };" ^
 "if ([string]$rule.Direction -ne 'Inbound' -or [string]$rule.Action -ne 'Allow' -or [string]$rule.Profile -ne 'Any') { return $false };" ^
 "$ports = @($rule | Get-NetFirewallPortFilter -ErrorAction Stop);" ^
 "$addresses = @($rule | Get-NetFirewallAddressFilter -ErrorAction Stop);" ^
 "$apps = @($rule | Get-NetFirewallApplicationFilter -ErrorAction Stop);" ^
 "$services = @($rule | Get-NetFirewallServiceFilter -ErrorAction Stop);" ^
 "if ($ports.Count -ne 1 -or $addresses.Count -ne 1 -or $apps.Count -ne 1 -or $services.Count -ne 1) { return $false };" ^
 "$protocol = [string]$ports[0].Protocol;" ^
 "$number = if ($spec.Protocol -eq 'TCP') { '6' } else { '17' };" ^
 "return (($protocol -eq $spec.Protocol -or $protocol -eq $number) -and" ^
 "(@($ports[0].LocalPort).Count -eq 1) -and ([string]$ports[0].LocalPort -eq [string]$spec.Port) -and" ^
 "([string]$ports[0].RemotePort -eq 'Any') -and" ^
 "([string]$addresses[0].LocalAddress -eq 'Any') -and ([string]$addresses[0].RemoteAddress -eq 'Any') -and" ^
 "([string]$apps[0].Program -eq 'Any') -and ([string]$services[0].Service -eq 'Any'));" ^
 "};" ^
 "try {" ^
 "$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent());" ^
 "if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {" ^
 "throw 'Can quyen Administrator. Click phai file va chon Run as administrator.';" ^
 "};" ^
 "$existing = @(Get-NetFirewallRule -PolicyStore PersistentStore -ErrorAction Stop);" ^
 "$failed = 0;" ^
 "foreach ($spec in $specs) {" ^
 "try {" ^
 "$matches = @($existing | Where-Object { $_.Name -eq $spec.Name });" ^
 "if ($matches.Count -gt 1) { throw 'Nhieu rule trung Name; giu nguyen de kiem tra thu cong.' };" ^
 "if ($matches.Count -eq 1) {" ^
 "if (-not (Test-ProjectRule $matches[0] $spec)) { throw 'Rule cung Name khac noi dung/chu so huu; khong ghi de.' };" ^
 "if ([string]$matches[0].Enabled -ne 'True') { throw 'Rule cua du an dang bi tat; giu nguyen, can kiem tra thu cong.' };" ^
 "Write-Host ('[DA CO] ' + $spec.Protocol + ' ' + $spec.Port + ': giu nguyen rule.');" ^
 "} else {" ^
 "New-NetFirewallRule -Name $spec.Name -DisplayName ('BerkeleyTimeSync - ' + $spec.Protocol + ' ' + $spec.Port) -Group $group -Description $owner -PolicyStore PersistentStore -Direction Inbound -Action Allow -Enabled True -Profile Any -Protocol $spec.Protocol -LocalPort $spec.Port -RemotePort Any -LocalAddress Any -RemoteAddress Any -Program Any -Service Any -InterfaceType Any -EdgeTraversalPolicy Block -ErrorAction Stop | Out-Null;" ^
 "Write-Host ('[DA THEM] ' + $spec.Protocol + ' ' + $spec.Port + ' / moi profile / moi IP nguon.');" ^
 "};" ^
 "} catch {" ^
 "$failed++;" ^
 "Write-Host ('[LOI] ' + $spec.Protocol + ' ' + $spec.Port + ': ' + $_.Exception.Message);" ^
 "};" ^
 "};" ^
 "if ($failed -gt 0) {" ^
 "Write-Host '[CHUA HOAN TAT] Mot so rule co the da duoc them. Xem ket qua tung cong; co the chay Dong_Cong_Firewall.bat de go rule cua du an.';" ^
 "exit 1;" ^
 "};" ^
 "Write-Host '[HOAN TAT] Da cau hinh hai rule. Rule ton tai den khi chay Dong_Cong_Firewall.bat.';" ^
 "Write-Host 'Rule Allow khong vuot qua rule Block hoac chinh sach quan tri. Chua phai ket qua kiem thu ket noi.';" ^
 "exit 0;" ^
 "} catch {" ^
 "Write-Host ('[LOI] ' + $_.Exception.Message);" ^
 "exit 1;" ^
 "}"
set "RESULT=%ERRORLEVEL%"
echo.
pause
endlocal & exit /b %RESULT%
