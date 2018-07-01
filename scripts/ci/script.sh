#!/bin/bash

set -x
set -e

python -mtests

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	codecov
fi

if [[ ! -z "$TRAVIS_TAG" ]]; then
	./scripts/build_package.sh
	./scripts/build_posix.sh
fi
