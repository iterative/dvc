@echo OFF

rmdir /Q /S dist
rmdir /Q /S WinPython
del /Q /S "dvc-*.exe"

if not exist WinPython-64bit-3.6.1.0Zero.exe (echo Error: Couldn't find WinPython installer. Please go to winpython.github.io, download WinPython-64bit-3.6.1.0Zero.exe and place it in project's root directory && goto:eof)

if not exist "C:\Program Files (x86)\Inno Setup 5\ISCC.exe" (echo Error: Couldn't find Inno Setup compiler. Please go to jrsoftware.org/isinfo.php and install Inno Setup 5 && goto:eof)

call .\WinPython-64bit-3.6.1.0Zero.exe /S /D=%cd%\WinPython

copy innosetup\addSymLinkPermissions.ps1 WinPython\
mkdir Winpython\bin
copy innosetup\dvc.bat WinPython\bin

cd WinPython
call scripts\python -m pip install -r ..\requirements.txt
cd ..

call WinPython\scripts\python setup.py sdist

cd WinPython
call scripts\python -m pip install ..\dist\dvc-0.8.2.tar.gz
cd ..

call "C:\Program Files (x86)\Inno Setup 5\iscc" innosetup\setup.iss
