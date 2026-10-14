@echo off
chcp 65001 > nul
echo ====================================================================
echo   DANG DONG CONG 8888 TRUONG TUONG LUA (GO BO QUY TAC BERKELEY)...
echo ====================================================================
netsh advfirewall firewall delete rule name="Berkeley Time Server"
echo.
if %errorlevel% == 0 (
    echo [THANH CONG] Da dong cong 8888 va go bo quy tac khoi Tuong lua!
) else (
    echo [THONG BAO] Khong tim thay quy tac hoac can quyen Administrator.
)
echo.
pause
