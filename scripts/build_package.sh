#!/bin/bash

set -e
set -x

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root" >&2
  exit 1
fi

echo 'PKG = "pip"' >dvc/utils/build.py

python -m pip install -U build setuptools>=58.2
python -m build --sdist --wheel --outdir dist/
