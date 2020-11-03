@echo Off

echo ====== Starting to build dvc installer for Windows... ======

if not exist dvc\NUL (echo Error: Please run this script from repository root && goto :error)

rmdir /Q /S build
rmdir /Q /S dist
del /Q /S dvc.spec
del /Q /S "dvc-*.exe"

where pip
if %errorlevel% neq 0 (echo Error: pip not found && goto :error)

where choco
if %errorlevel% neq 0 (echo Error: choco not found && goto :error)

choco install InnoSetup
call refreshenv
where iscc
if %errorlevel% neq 0 (echo Error: Couldn't find Inno Setup compiler. && goto :error)

echo ====== Installing requirements... ======
echo PKG = "exe" > dvc\utils\build.py
call pip install .[all] || goto :error
call pip install -r scripts\build-requirements.txt || goto :error
call dvc pull || goto :error

echo ====== Building dvc binary... ======
call pyinstaller --additional-hooks-dir scripts\hooks dvc/__main__.py --name dvc --specpath build

echo ====== Testing dvc binary... ======
call dist\dvc\dvc.exe version || goto :error

echo ====== Copying additional files... ======
copy scripts\innosetup\addSymLinkPermissions.ps1 dist\ || goto :error

echo ====== Building dvc installer... ======
set PYTHONPATH=%cd%
call python scripts\innosetup\config_gen.py || goto :error
call iscc scripts\innosetup\setup.iss || goto :error

echo ====== DONE ======
goto :EOF

:error
echo ====== FAIL ======
exit /b 1
