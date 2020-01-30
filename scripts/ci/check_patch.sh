#!/bin/bash

set -x
set -e

pip install Pygments collective.checkdocs pre-commit

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any formatting errors
err=0
pre-commit run --all-files black || err=1
pre-commit run --all-files flake8 || err=1
exit $err
