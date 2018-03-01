@ECHO OFF

SETLOCAL EnableDelayedExpansion

if NOT DEFINED JAVA_HOME goto err

for /f tokens^=2-5^ delims^=.-_^" %%j in ('"%JAVA_HOME%\bin\java" -fullversion 2^>^&1') do set "JAVA_VERSION=%%j.%%k"

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

REM GC logging default values
SET GC_LOG_DIR=%CRATE_HOME%\logs
SET GC_LOG_SIZE=64m
SET GC_LOG_FILES=16

REM Set CRATE_DISABLE_GC_LOGGING=1 to disable GC logging
if NOT DEFINED "%CRATE_DISABLE_GC_LOGGING%" (

  REM GC logging requires 16x64mb = 1g of free disk space
  IF DEFINED %CRATE_GC_LOG_DIR% (SET GC_LOG_DIR=!CRATE_GC_LOG_DIR!)
  IF DEFINED %CRATE_GC_LOG_SIZE% (SET GC_LOG_SIZE=!CRATE_GC_LOG_SIZE!)
  IF DEFINED %CRATE_GC_LOG_FILES% (SET GC_LOG_FILES=!CRATE_GC_LOG_FILES!)

  SET LOGGC=!GC_LOG_DIR!\gc.log
  ECHO %JAVA_VERSION%

  IF "%JAVA_VERSION%" == "9.0" (
    SET JAVA_OPTS=!JAVA_OPTS! -Xlog:gc*,gc+age=trace,safepoint:file=\"!LOGGC!\":utctime,pid,tags:filecount=!GC_LOG_FILES!,filesize=!GC_LOG_SIZE!
  )
  IF "%JAVA_VERSION%" == "1.8" (
    SET JAVA_OPTS=!JAVA_OPTS! -Xloggc:!LOGGC!
    SET JAVA_OPTS=!JAVA_OPTS! -XX:+PrintGCDetails
    SET JAVA_OPTS=!JAVA_OPTS! -XX:+PrintGCDateStamps
    SET JAVA_OPTS=!JAVA_OPTS! -XX:+PrintTenuringDistribution
    SET JAVA_OPTS=!JAVA_OPTS! -XX:+PrintGCApplicationStoppedTime
    SET JAVA_OPTS=!JAVA_OPTS! -XX:+UseGCLogFileRotation
    SET JAVA_OPTS=!JAVA_OPTS! -XX:NumberOfGCLogFiles=!GC_LOG_FILES!
    SET JAVA_OPTS=!JAVA_OPTS! -XX:GCLogFileSize=!GC_LOG_SIZE!
  )
)

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

REM Dump heap on OOM
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
if NOT "%CRATE_HEAP_DUMP_PATH%" == "" (
    set JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=%CRATE_HEAP_DUMP_PATH%
)

if "%CRATE_CLASSPATH%" == "" (
    set CRATE_CLASSPATH=%CRATE_HOME%/lib/*;%CRATE_HOME%/lib/enterprise/*
) else (
    ECHO Error: Don't modify the classpath with CRATE_CLASSPATH. 1>&2
    ECHO Add plugins and their dependencies into the plugins/ folder instead. 1>&2
    EXIT /B 1
)
set CRATE_PARAMS=-Cpath.home="%CRATE_HOME%"

set params='%*'

for /F "usebackq tokens=* delims= " %%A in (!params!) do (
    set param=%%A

    if "!param:~0,5!" equ "-Des." (
        echo "Support for defining Crate specific settings with the -D option and the es prefix has been dropped."
        echo "Please use the -C option to configure Crate."
        EXIT /B 1
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
