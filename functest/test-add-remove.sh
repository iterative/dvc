#!/bin/bash

set -e

source common.sh

dvc_create_repo

dvc add data/foo
dvc_check_files data/foo.dvc data/foo

dvc remove data/foo
if [ -f "data/foo" ]; then
    echo "data/foo was not removed"
    dvc_fail
fi
if [ -f "data/foo.dvc" ]; then
    echo "data/foo.dvc was not removed"
    dvc_fail
fi

dvc_pass
