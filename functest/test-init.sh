#!/bin/bash

set -e

source common.sh

dvc_create_git_repo

dvc init

DIRS="data .dvc/cache .dvc/state"
FILES=".dvc/config"

dvc_check_dirs $DIRS
dvc_check_files $FILES

dvc_pass
