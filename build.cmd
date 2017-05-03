@echo OFF
rmdir /Q /S nuitka.build
del /Q /S "dvc-*.exe"
@echo This will take a while. Go get yourself a cup of tee...
call nuitka --standalone --output-dir nuitka.build dvc.py
call "C:\Program Files (x86)\Inno Setup 5\iscc" innosetup/setup.iss
rmdir /Q /S nuitka.build
