#!/bin/bash

test_description='Basic file tracking'
source "$(dirname "$0")"/init.sh

# init a repo and create a test file
dvc init --no-scm --quiet
echo "test 1" > file1.txt

test_expect_success 'dvc add file1.txt' '
    dvc add file1.txt &&
    [[ -f file1.txt.dvc ]] &&
    cat file1.txt.dvc | grep "md5: $(md5sum file1.txt | cut -d" " -f1)"
    [[ -f .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d ]] &&
    [[ -z $(diff file1.txt .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d) ]]
'

test_expect_success 'Remove file1.txt and restore it from cache' '
    rm file1.txt &&
    dvc status | grep "deleted:" &&
    dvc checkout file1.txt.dvc &&
    [[ -f file1.txt ]] &&
    [[ $(cat file1.txt) == "test 1" ]] &&
    dvc status file1.txt.dvc | grep "up to date"
'

test_expect_success 'Modify file1.txt and restore it from cache' '
    echo "xyz" > file1.txt &&
    dvc status | grep "modified:" &&
    dvc checkout -f file1.txt.dvc &&
    [[ $(cat file1.txt) == "test 1" ]] &&
    dvc status file1.txt.dvc | grep "up to date"
'

test_expect_success 'Stop tracking file1.txt' '
    rm file1.txt.dvc &&
    dvc status | grep "up to date" &&
    [[ -f .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d ]] &&
    dvc gc -f &&
    [[ ! -f .dvc/cache/24/90a3d39b0004e4afeb517ef0ddbe2d ]] &&
    dvc status | grep "up to date"
'

test_done
