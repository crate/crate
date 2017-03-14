@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set CRATE_HOME=%%~dpfI

REM Ensure UTF-8 encoding by default (e.g. filenames)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

set CRATE_CLASSPATH=%CRATE_HOME%/lib/*.jar
set CRATE_PARAMS=-Dcrate -Des.path.home="%CRATE_HOME%"

setlocal enabledelayedexpansion
set params='%*'

for /F "usebackq tokens=* delims= " %%A in (!params!) do (
    set param=%%A

    if "x!newparams!" neq "x" (
        set newparams=!newparams! !param!
    ) else (
        set newparams=!param!
    )
)

"%JAVA_HOME%\bin\java" -Des.path.home="%CRATE_HOME%" -cp "%CRATE_CLASSPATH%" "io.crate.migration.MigrationTool" !$newparams!
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally

ENDLOCAL
