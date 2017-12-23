#!/bin/bash

set -e

source common.sh

dvc_create_repo

dvc run -D code/code.sh -d data/foo -o data/foo1 bash code/code.sh data/foo data/foo1

rm -f data/foo
dvc checkout
dvc_check_files data/foo
dvc_pass
