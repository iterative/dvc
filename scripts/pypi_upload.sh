#!/bin/bash

set -e

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root"
  exit 1
fi

rm -rf dist/
pip install twine
python -m pip install --user build
python -m build --sdist --wheel --outdir dist/
twine upload dist/*
