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

        PY_VER=$(python -c 'import sys; print(sys.version_info[0:2])')
        if [[ $PY_VER  == '(3, 7)' ]]; then
            ./scripts/build_posix.sh
        else
            echo "Not building dvc binary package on Python 3.7, since it is "\
                 "not supported by PyInstaller yet."
        fi
fi
