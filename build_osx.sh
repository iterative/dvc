echo "building osx"
# rm -rf build/ dist/
pyinstaller --onefile --console \
  --clean \
  --noconfirm \
  --specpath dist/ \
  dvc2.py
mv dist/dvc2 dist/dvc
