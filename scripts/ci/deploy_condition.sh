#!/bin/bash

set -e

python_ver="$(python -c 'import sys; print(sys.version_info[0])')"

if [[ $python_ver != "2" ]]; then
    echo false
    exit 0
fi

if [[ $TRAVIS_OS_NAME == "osx" && $TRAVIS_OSX_IMAGE != "xcode10.1" ]]; then
    echo false
    exit 0
fi

echo true
