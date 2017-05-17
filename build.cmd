@echo OFF

echo ====== Starting to build dvc installer for Windows... ======

rmdir /Q /S dist
rmdir /Q /S WinPython
del /Q /S "dvc-*.exe"

if not exist WinPython-64bit-3.6.1.0Zero.exe (echo Error: Couldn't find WinPython installer. Please go to winpython.github.io, download WinPython-64bit-3.6.1.0Zero.exe and place it in project's root directory && goto:error)

if not exist "C:\Program Files (x86)\Inno Setup 5\ISCC.exe" (echo Error: Couldn't find Inno Setup compiler. Please go to jrsoftware.org/isinfo.php and install Inno Setup 5 && goto:error)

echo ====== Installing WinPython... ======
call .\WinPython-64bit-3.6.1.0Zero.exe /S /D=%cd%\WinPython || goto :error

echo ====== Copying additional files... ======
copy innosetup\addSymLinkPermissions.ps1 WinPython\ || goto :error
mkdir Winpython\bin || goto :error
copy innosetup\dvc.bat WinPython\bin || goto :error

echo ====== Installing requirements... ======
cd WinPython || goto :error
call scripts\python -m pip install -r ..\requirements.txt || goto :error
cd .. || goto :error

echo ====== Building dvc sdist... ======
call WinPython\scripts\python setup.py sdist || goto :error

echo ====== Installing dvc sdist into WinPython... ======
cd WinPython || goto :error
for %%s in ("..\dist\dvc-*.tar.gz") do call scripts\python -m pip install "%%s" || goto :error
cd .. || goto :error

echo ====== Building dvc installer... ======
cd innosetup || goto :error
call ..\WinPython\scripts\python config_gen.py || goto :error
cd .. || goto :error
call "C:\Program Files (x86)\Inno Setup 5\iscc" innosetup\setup.iss || goto :error

echo ====== DONE ======
goto :EOF

:error
echo ====== FAIL ======
exit /b 1
