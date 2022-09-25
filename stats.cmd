@echo off
cloc . --exclude-dir=Judgments,dumps,PDFServicesSDKSamples --exclude-ext=json,txt,md --counted=evaluated_files
echo.
echo Evaluated files:
type evaluated_files
del /F evaluated_files
echo.
pause