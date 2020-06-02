#!/bin/bash

set -x
set -e

pip install Pygments collective.checkdocs pre-commit .

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any formatting errors
pre-commit run --all-files

# check bash completio
python -m dvc completion -o scripts/completion/dvc.bash
[[ -z "$(git diff -U0 -- scripts/completion/dvc.bash)" ]] || exit 1
