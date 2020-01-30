#!/bin/bash

set -e
set -x

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root" >&2
  exit 1
fi

echo 'PKG = "pip"' >dvc/utils/build.py

python setup.py sdist
python setup.py bdist_wheel --universal

# Make sure we have a correct version
if [[ -n "$TRAVIS_TAG" ]]; then
  pip uninstall -y dvc
  if which dvc; then
    echo "ERROR: dvc command still exists! Unable to verify dvc version." >&2
    exit 1
  fi
  pip install dist/dvc-*.whl
  if [[ "$(dvc --version)" != "$TRAVIS_TAG" ]]; then
    echo "ERROR: 'dvc --version'$(dvc -V) doesn't match '$TRAVIS_TAG'" >&2
    exit 1
  fi
  pip uninstall -y dvc
fi
