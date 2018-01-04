#!/bin/bash

set -e

source common.sh

dvc_create_repo

dvc add data/foo
dvc run -f Dvcfile -d code/code.sh -d data/foo -o data/foo1 bash code/code.sh data/foo data/foo1
cat Dvcfile | grep 'locked' | grep 'false' || dvc_fatal "stage locked after 'dvc run'"
dvc lock
cat Dvcfile | grep 'locked' | grep 'true' || dvc_fatal "stage not locked after 'dvc lock'"
dvc lock --unlock
cat Dvcfile | grep 'locked' | grep 'false' || dvc_fatal "stage locked after 'dvc lock --unlock'"
dvc_pass
