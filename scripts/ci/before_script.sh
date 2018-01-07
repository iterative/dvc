#!/bin/bash

if [[ "$TRAVIS_OS_NAME" == "linux" && \
      "$(python -c 'import sys; print(sys.version_info[0])')" == "2" && \
      "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_BRANCH" == "master" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	curl -L https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64 > ./cc-test-reporter
	chmod +x ./cc-test-reporter
	./cc-test-reporter before-build
fi
