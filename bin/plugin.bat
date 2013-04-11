@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set CRATE_HOME=%%~dpfI


"%JAVA_HOME%\bin\java" %JAVA_OPTS% -Xmx64m -Xms16m -Des.path.home="%CRATE_HOME%" -cp "%CRATE_HOME%/lib/*;" "org.elasticsearch.plugins.PluginManager" %*
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL
