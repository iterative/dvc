#!/bin/bash

set -x
set -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
	pyenv install $PYTHON_VER
	pyenv global $PYTHON_VER
fi
