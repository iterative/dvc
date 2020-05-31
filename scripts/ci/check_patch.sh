#!/bin/bash

set -x
set -e

pip install Pygments collective.checkdocs pre-commit .

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any formatting errors
pre-commit run --all-files

# check bash completion
python scripts/completion/bash.py
completion_diff="$(git diff -U0 -- scripts/completion/dvc.bash)"
if [[ -n "$completion_diff" ]]; then
  echo "ERROR: bash completion changed:" >&2
  echo "$completion_diff" >&2
  exit 1
fi
