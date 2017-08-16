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

REM Disables explicit GC
set JAVA_OPTS=%JAVA_OPTS% -XX:+DisableExplicitGC

REM Use our provided JNA always versus the system one
set JAVA_OPTS=%JAVA_OPTS% -Djna.nosys=true

REM Ensure UTF-8 encoding by default (e.g. filenames)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

REM log4j options
set JAVA_OPTS=%JAVA_OPTS% -Dlog4j.shutdownHookEnabled=false -Dlog4j2.disable.jmx=true -Dlog4j.skipJansi=true

REM Disable netty recycler
set JAVA_OPTS=%JAVA_OPTS% -Dio.netty.noUnsafe=true -Dio.netty.noKeySetOptimization=true -Dio.netty.recycler.maxCapacityPerThread=0

if "%CRATE_CLASSPATH%" == "" (
    set CRATE_CLASSPATH=%CRATE_HOME%/lib/*;%CRATE_HOME%/lib/enterprise/*;%CRATE_HOME%/lib/sigar/*
) else (
    ECHO Error: Don't modify the classpath with CRATE_CLASSPATH. 1>&2
    ECHO Add plugins and their dependencies into the plugins/ folder instead. 1>&2
    EXIT /B 1
)

if "%CRATE_HEAP_SIZE%" == "" (
    for /f %%a in ('"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CRATE_JAVA_OPTS% -cp "%CRATE_CLASSPATH%" "io.crate.CrateJavaInfo" -m') do set "MAX_HEAP_SIZE=%%a"
    echo "WARNING: No CRATE_HEAP_SIZE environment variable was set, using default maximum value %MAX_HEAP_SIZE% as minimum and maximum java HEAP size"
    set JAVA_OPTS="%JAVA_OPTS% -Xms%MAX_HEAP_SIZE% -Xmx%MAX_HEAP_SIZE%"
)

set CRATE_PARAMS=-Epath.home="%CRATE_HOME%"

setlocal enabledelayedexpansion
set params='%*'

for /F "usebackq tokens=* delims= " %%A in (!params!) do (
    set param=%%A

    if "!param:~0,5!" equ "-Des." (
        echo "Support for defining Crate specific settings with the -D option and the es prefix has been dropped."
        echo "Please use the -C option to configure Crate."
        EXIT /B 1
    ) else if "!param:~0,2!" equ "-C" (
        set param=!param:-C=-E!
    )

    if "x!newparams!" neq "x" (
        set newparams=!newparams! !param!
    ) else (
        set newparams=!param!
    )
)

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CRATE_JAVA_OPTS% -cp "%CRATE_CLASSPATH%" "io.crate.bootstrap.CrateDB" %CRATE_PARAMS% !newparams!
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
