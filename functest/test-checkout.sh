#!/bin/bash

set -e

source common.sh

dvc_create_repo

rm -f data/foo
dvc checkout
dvc_check_files data/foo
dvc_pass
