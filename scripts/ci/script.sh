#!/bin/bash

set -x
set -e

python -mtests

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	codecov
fi

if [[ ! -z "$TRAVIS_TAG" ]]; then
	if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
		./scripts/build_macos.sh
	else
		./scripts/build_linux.sh
	fi

	./scripts/build_package.sh
fi
