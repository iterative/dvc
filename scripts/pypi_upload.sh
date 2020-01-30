#!/bin/bash

set -e

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root"
  exit 1
fi

rm -rf dist/
pip install twine
python setup.py sdist bdist_wheel --universal
twine upload dist/*
