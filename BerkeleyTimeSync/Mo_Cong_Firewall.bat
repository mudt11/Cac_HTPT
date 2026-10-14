@echo off
chcp 65001 > nul
echo ====================================================================
echo   DANG MO CONG 8888 TRUONG TUONG LUA CHO BERKELEY TIME SERVER...
echo ====================================================================
netsh advfirewall firewall add rule name="Berkeley Time Server" dir=in action=allow protocol=TCP localport=8888
echo.
if %errorlevel% == 0 (
    echo [THANH CONG] Da mo cong TCP 8888 thanh cong!
    echo Cac may tram (Clients) gio day da co the ket noi toi Server.
) else (
    echo [LOI] Khong the mo cong!
    echo VUI LONG CLICK CHUOT PHAI VAO FILE NAY VA CHON "RUN AS ADMINISTRATOR"!
)
echo.
pause
