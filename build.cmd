@echo OFF
set Miniconda_dir=C:\DVC
set Miniconda_bin=Miniconda3-latest-Windows-x86_64.exe
set Miniconda_url=https://repo.continuum.io/miniconda/Miniconda3-latest-Windows-x86_64.exe 
set Src_dir=%cd%

rmdir /Q /S %Miniconda_dir%
del /Q /S %Miniconda_bin%
del /Q /S "dvc-*.exe"

call powershell.exe -Command (new-object System.Net.WebClient).DownloadFile('%Miniconda_url%','%Miniconda_bin%')

REM NOTE: This is extremely important, that we use same directory that we will install dvc into.
REM 	  The reason is that dvc.exe will have hardcoded path to python interpreter and thus we need
REM 	  to make sure that it is the same everywhere(C:\DVC).
call .\%Miniconda_bin% /InstallationType=JustMe /RegisterPython=0 /S /D=%Miniconda_dir%

copy innosetup\addSymLinkPermissions.ps1 %Miniconda_dir%\

pushd %Miniconda_dir%
call .\python -m pip install -r %Src_dir%\requirements.txt
popd

call %Miniconda_dir%\python setup.py sdist

pushd %Miniconda_dir%
call .\python -m pip install %Src_dir%\dist\dvc-0.8.1.tar.gz
popd

call "C:\Program Files (x86)\Inno Setup 5\iscc" innosetup\setup.iss
