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

REM When running under Java 7
REM JAVA_OPTS=%JAVA_OPTS% -XX:+UseCondCardMark

REM GC logging options -- uncomment to enable
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCDetails
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCTimeStamps
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintClassHistogram
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintTenuringDistribution
REM JAVA_OPTS=%JAVA_OPTS% -XX:+PrintGCApplicationStoppedTime
REM JAVA_OPTS=%JAVA_OPTS% -Xloggc:/var/log/crate/gc.log

REM Causes the JVM to dump its heap on OutOfMemory.
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
REM The path to the heap dump location, note directory must exists and have enough
REM space for a full heap dump.
REM JAVA_OPTS=%JAVA_OPTS% -XX:HeapDumpPath=$CRATE_HOME/logs/heapdump.hprof

REM Ensure UTF-8 encoding by default (e.g. filenames)
set JAVA_OPTS=%JAVA_OPTS% -Dfile.encoding=UTF-8

set CRATE_CLASSPATH=%CRATE_CLASSPATH%;%CRATE_HOME%/lib/crate-*.jar;%CRATE_HOME%/lib/*;%CRATE_HOME%/lib/sigar/*
set CRATE_PARAMS=-Dcrate -Des.path.home="%CRATE_HOME%" -Des.config="%CRATE_HOME%/config/crate.yml"

"%JAVA_HOME%\bin\java" %JAVA_OPTS% %CRATE_JAVA_OPTS% %CRATE_PARAMS% %* -cp "%CRATE_CLASSPATH%" "io.crate.bootstrap.CrateF"
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL