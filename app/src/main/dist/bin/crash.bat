@echo off

SETLOCAL

reg query "hkcu\software\python" > NUL 2>&1
IF ERRORLEVEL 1 (
  REM no user python
  reg query "hklm\software\python" > NUL 2>&1
  IF ERRORLEVEL 1 (
    REM no system python
    goto :NOPYTHON
  ) ELSE (
    goto :HASPYTHON
  )
) ELSE (
    goto :HASPYTHON
)

:NOPYTHON
echo "No python executable found. Please install python in order to use crash"
exit /b %ERRORLEVEL%

:HASPYTHON
REM Must set these first thing due to bug in Windows 7 when batch script filename has spaces in it
SET BATCH_SCRIPT_FOLDER_PATHNAME=%~dp0
"%BATCH_SCRIPT_FOLDER_PATHNAME%/crash_standalone.zip.py" %*
exit /b %ERRORLEVEL%

ENDLOCAL
