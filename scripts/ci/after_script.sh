#!/bin/bash

set -x
set -e

if [[ "$TRAVIS_OS_NAME" == "linux" && \
      "$(python -c 'import sys; print(sys.version_info[0])')" == "2" && \
      "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_BRANCH" == "master" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	coverage xml
	./cc-test-reporter after-build --exit-code $TRAVIS_TEST_RESULT
fi
