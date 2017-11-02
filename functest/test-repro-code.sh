#!/bin/bash

set -e

source common.sh

dvc_create_repo

dvc_info 'Copy foo into foo1'
dvc run bash code/code.sh data/foo data/foo1

dvc_info 'Modify code'
echo " " >> code/code.sh 
git commit -am 'Change code'
    
dvc_info 'Reproduce foo1'
dvc repro data/foo1
dvc_check_files data/foo1
if [ "$(cat data/foo1)" != "foo" ]; then
        dvc_fail
fi

dvc_info 'Modify foo'
dvc remove data/foo
dvc import $DATA_CACHE/bar data/foo

dvc_info 'Set default target'
dvc config global.target data/foo1
git commit -am 'Set default target'

dvc_info 'Reproduce foo1 as default target'
dvc repro
dvc_check_files data/foo1
if [ "$(cat data/foo1)" != "bar" ]; then
        dvc_fail
fi

dvc_pass
