#!/bin/bash

set -e

source common.sh

dvc_create_git_repo

dvc init

DIRS="data .cache .state"
FILES="dvc.conf"

dvc_check_dirs $DIRS
dvc_check_files $FILES

dvc_pass
