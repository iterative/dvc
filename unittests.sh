#!/bin/bash
set -e

nosetests --cover-inclusive --cover-erase --cover-package=dvc --with-coverage

if [ "$1" = "report" ]; then
    if [ -z "$CODECLIMATE_REPO_TOKEN" ]; then
        echo "Error: CODECLIMATE_REPO_TOKEN is not defined! Reporting faied."
    else
        codeclimate-test-reporter --token $CODECLIMATE_REPO_TOKEN
    fi
fi

