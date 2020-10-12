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
