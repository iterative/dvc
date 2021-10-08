#!/bin/bash

set -e

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root"
  exit 1
fi

rm -rf dist/
pip install twine
python -m pip install -U build setuptools>=58.2
python -m build --sdist --wheel --outdir dist/
twine upload dist/*
