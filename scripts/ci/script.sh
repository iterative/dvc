#!/bin/bash

set -x
set -e

./unittests.sh

if [[ "$TRAVIS_OS_NAME" == "linux" && \
      "$(python -c 'import sys; print(sys.version_info[0])')" == "2" && \
      "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_BRANCH" == "master" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
        cat .coverage
	codeclimate-test-reporter --file .coverage --debug --token $CC_TEST_REPORTER_ID
fi

if [[ ! -z "$TRAVIS_TAG" ]]; then
	if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
		./scripts/build_macos.sh
	else
		./scripts/build_linux.sh
	fi

	./scripts/build_package.sh
fi
