#!/bin/bash

set -x
set -e

python -mtests

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
  "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
  pip install codecov
  codecov
fi
