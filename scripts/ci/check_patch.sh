#!/bin/bash

set -x
set -e

python3 -m pip install --user Pygments collective.checkdocs pre-commit

# stop the build if there are any readme formatting errors
python3 setup.py checkdocs

# stop the build if there are any formatting errors
GO111MODULE=on pre-commit run --all-files
