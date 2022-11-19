@echo off
cloc . --exclude-dir=Judgments,dumps,PDFServicesSDKSamples,docs,SampleJudgments --exclude-ext=json,txt,md,csv,pdf,ini --counted=evaluated_files
echo.
echo Evaluated files:
type evaluated_files
del /F evaluated_files
echo.
pause