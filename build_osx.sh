echo "building osx"
# rm -rf build/ dist/
pyinstaller --onefile --console \
  --debug \
  --clean \
  --noconfirm \
  --specpath dist/ \
  dvc2.spec
mv dist/dvc2 dist/dvc
