@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set CRATE_HOME=%%~dpfI


REM ***** JAVA options *****

if "%CRATE_MIN_MEM%" == "" (
set CRATE_MIN_MEM=256m
)

if "%CRATE_MAX_MEM%" == "" (
set CRATE_MAX_MEM=1g
)

if NOT "%CRATE_HEAP_SIZE%" == "" (
set CRATE_MIN_MEM=%CRATE_HEAP_SIZE%
set CRATE_MAX_MEM=%CRATE_HEAP_SIZE%
)

set JAVA_OPTS=%JAVA_OPTS% -Xms%CRATE_MIN_MEM% -Xmx%CRATE_MAX_MEM%

if NOT "%CRATE_HEAP_NEWSIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -Xmn%CRATE_HEAP_NEWSIZE%
)

if NOT "%CRATE_DIRECT_SIZE%" == "" (
set JAVA_OPTS=%JAVA_OPTS% -XX:MaxDirectMemorySize=%CRATE_DIRECT_SIZE%
)

set JAVA_OPTS=%JAVA_OPTS% -Xss256k

REM Enable aggressive optimizations in the JVM
REM    - Disabled by default as it might cause the JVM to crash
REM set JAVA_OPTS=%JAVA_OPTS% -XX:+AggressiveOpts

set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly

REM GC logging options -- uncomment to enable
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimeStamps
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
REM JAVA_OPTS=%JAVA_OPTS% -Xloggc:/var/log/crate/gc.log

REM Ensure UTF-8 encoding by default (e.g. filenames)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

if "%CRATE_CLASSPATH%" == "" (
    set CRATE_CLASSPATH=%CRATE_HOME%/lib/crate-app-@version@.jar;%CRATE_HOME%/lib/*
) else (
    ECHO Error: Don't modify the classpath with CRATE_CLASSPATH. 1>&2
    ECHO Add plugins and their dependencies into the plugins/ folder instead. 1>&2
    EXIT /B 1
)
set CRATE_PARAMS=-Dcrate -Des.path.home="%CRATE_HOME%"

setlocal enabledelayedexpansion
set params='%*'

for /F "usebackq tokens=* delims= " %%A in (!params!) do (
    set param=%%A

    if "!param:~0,5!" equ "-Des." (
        echo "Setting Crate specific settings with the -D option and the es prefix has been deprecated."
        echo "Please use the -C option to configure Crate."
    ) else if "!param:~0,2!" equ "-C" (
        set param=!param:-C=-Des.!
    )

    if "x!newparams!" neq "x" (
        set newparams=!newparams! !param!
    ) else (
        set newparams=!param!
    )
)

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CRATE_JAVA_OPTS% %CRATE_PARAMS% !newparams! -cp "%CRATE_CLASSPATH%" "io.crate.bootstrap.CrateDB"
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
