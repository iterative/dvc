@echo Off

echo ====== Starting to build dvc installer for Windows... ======

if not exist dvc\NUL (echo Error: Please run this script from repository root && goto :error)

rmdir /Q /S build
rmdir /Q /S dist
del /Q /S dvc.spec
del /Q /S "dvc-*.exe"

where pip
if %errorlevel% neq 0 (echo Error: pip not found && goto :error)

if not exist "C:\Program Files (x86)\Inno Setup 5\ISCC.exe" (echo Error: Couldn't find Inno Setup compiler. Please go to jrsoftware.org/isinfo.php and install Inno Setup 5 && goto :error)

echo ====== Installing requirements... ======
call pip install -e .[all] || goto :error
call pip install pyinstaller || goto :error

echo ====== Building dvc binary... ======
call pyinstaller --additional-hooks-dir scripts\hooks dvc/__main__.py --name dvc --specpath build

echo ====== Copying additional files... ======
copy scripts\innosetup\addSymLinkPermissions.ps1 dist\ || goto :error

echo ====== Building dvc installer... ======
set PYTHONPATH=%cd%
call python scripts\innosetup\config_gen.py || goto :error
call "C:\Program Files (x86)\Inno Setup 5\iscc" scripts\innosetup\setup.iss || goto :error

echo ====== DONE ======
goto :EOF

:error
echo ====== FAIL ======
exit /b 1
