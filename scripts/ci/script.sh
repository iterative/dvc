#!/bin/bash

set -x
set -e

python -mtests

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" && \
      "$(python -c 'import sys; print(sys.version_info[0])')" == '2' ]]; then
	pip install codecov
	codecov
fi
