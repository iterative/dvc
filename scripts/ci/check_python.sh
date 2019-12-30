#!/bin/bash

set -e
set -x

if [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
    where python
    where pip
else
    which python
    which pip
fi

py_ver="$(python -c 'import sys; print(sys.version[0])')"

if [[ "$py_ver" != 3 ]]; then
  exit 1
fi
