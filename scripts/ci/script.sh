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

if [[ -n "$TRAVIS_TAG" || "$TRAVIS_EVENT_TYPE" == "cron" ]]; then
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
        if [[ "$PY_VER" != '(3, 7)' ]]; then
            ./scripts/build_posix.sh
        else
            echo "Not building dvc binary package on Python 3.7, since it is "\
                 "not supported by PyInstaller yet."
        fi
fi
