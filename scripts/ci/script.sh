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

if [[ -z "$TRAVIS_TAG" && "$TRAVIS_EVENT_TYPE" != "cron" ]]; then
    echo "Skipping building package."
    exit 0
fi

./scripts/build_package.sh

if [[ -n "$TRAVIS_TAG" ]]; then
    # Test version
    pip uninstall -y dvc
    if [ -x "$(command -v dvc)" ]; then
        echo "ERROR: dvc command already exists!!!"
        exit 1
    fi

    pip install dist/dvc-*.whl
    if [[ "$(dvc --version)" != "$TRAVIS_TAG" ]]; then
        echo "ERROR: 'dvc --version'$(dvc -V) doesn't match '$TRAVIS_TAG'"
        exit 1
    fi
    pip uninstall -y dvc
fi

PY_VER=$(python -c 'import sys; print(sys.version_info[0:2])')
if [[ "$PY_VER" != '(2, 7)' ]]; then
    echo "Skipping building binary package because of a wrong python version."
    exit 0
fi

if [[ "$TRAVIS_OS_NAME" == "osx" && "$TRAVIS_OSX_IMAGE" != "xcode7.3" ]]; then
    echo "Skipping building binary package because of a wrong xcode version."
    exit 0
fi

./scripts/build_posix.sh
