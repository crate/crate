@echo off

SETLOCAL

Python.exe -c '1+1'
IF ERRORLEVEL 0 (
  goto :HASPYTHON
) ELSE (
  goto :NOPYTHON
)

:NOPYTHON
echo "No python executable found. Please install python in order to use crash"
exit /b %ERRORLEVEL%

:HASPYTHON
REM Must set these first thing due to bug in Windows 7 when batch script filename has spaces in it
SET BATCH_SCRIPT_FOLDER_PATHNAME=%~dp0
Python.exe "%BATCH_SCRIPT_FOLDER_PATHNAME%/crash_standalone" %*
exit /b %ERRORLEVEL%

ENDLOCAL
