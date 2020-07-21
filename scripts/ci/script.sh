#!/bin/bash

set -x
set -e

python -m tests tests/func/test_{get,import,ls,update,api}.py

if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
  pip install codecov
  codecov
fi
