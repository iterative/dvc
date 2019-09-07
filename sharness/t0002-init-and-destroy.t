#!/bin/bash

test_description='Commands init and destroy'
source "$(dirname "$0")"/init.sh

test_expect_success 'dvc init --no-scm' '
    dvc init --no-scm &&
    [[ -d .dvc ]] &&
    [[ -d .dvc/cache ]] &&
    [[ -f .dvc/config ]]
'

test_expect_success 'Track a simple file' '
    echo "test 1" > file1.txt &&
    dvc add file1.txt &&
    [[ -f file1.txt.dvc ]] &&
    [[ -f .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d ]] &&
    [[ -z $(diff file1.txt .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d) ]]
'

test_expect_success 'dvc destroy' '
    dvc destroy -f &&
    [[ ! -d .dvc ]] &&
    [[ ! -f file1.txt.dvc ]] &&
    [[ -f file1.txt ]]
'

test_done
