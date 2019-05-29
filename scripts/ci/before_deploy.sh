#!/bin/bash

set -x
set -e

if [[ -z "$TRAVIS_TAG" && "$TRAVIS_EVENT_TYPE" != "cron" ]]; then
    echo "Skipping building package."
    exit 0
fi

./scripts/build_package.sh

if [[ -n "$TRAVIS_TAG" ]]; then
    # Test version
    pip uninstall -y dvc
    pip install dist/dvc-*.whl
    if [[ "$(dvc --version)" != "$TRAVIS_TAG" ]]; then
        echo "ERROR: 'dvc --version'$(dvc -V) doesn't match '$TRAVIS_TAG'"
        exit 1
    fi
    pip uninstall -y dvc
fi

PY_VER=$(python -c 'import sys; print(sys.version_info[0:2])')
if [[ "$PY_VER" != '(3, 7)' ]]; then
    echo "Skipping building binary package because of a wrong python version."
    exit 0
fi

if [[ "$TRAVIS_OS_NAME" == "osx" && "$TRAVIS_OSX_IMAGE" != "xcode7.3" ]]; then
    echo "Skipping building binary package because of a wrong xcode version."
    exit 0
fi

./scripts/build_posix.sh
