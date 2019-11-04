#!/bin/bash

set -x
set -e

pip install Pygments collective.checkdocs pre-commit 

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any formatting errors
pre-commit run --all-files
