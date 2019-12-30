#!/bin/bash

set -e

if [[ $TRAVIS_OS_NAME == "osx" && $TRAVIS_OSX_IMAGE != "xcode8.3" ]]; then
    echo false
    exit 0
fi

echo true
