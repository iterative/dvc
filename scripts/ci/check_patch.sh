#!/bin/bash

set -x
set -e

pip install Pygments collective.checkdocs flake8 'black==19.3b0'

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any black or flake8 errors
black ./ --check
flake8 ./
