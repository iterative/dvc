#!/bin/bash

set -e

source common.sh

dvc_create_repo

ORIG_CACHE=$(ls .dvc/cache)

DUMMY_CACHE+=" .dvc/cache/1"
DUMMY_CACHE+=" .dvc/cache/2"
DUMMY_CACHE+=" .dvc/cache/3"

dvc_info "Create dummy cache files"
touch $DUMMY_CACHE
if [ $? -ne 0 ]; then
	dvc_error "Failed to create cache files"
	dvc_fail
fi

dvc_info "Run 'dvc gc'"
dvc gc

dvc_info "Check that we didn't break good cache"
dvc_check_dirs ".dvc/cache"
pushd .dvc/cache
dvc_check_files $ORIG_CACHE
popd

dvc_info "Check that dummy cache files were removed"
for d in $DUMMY_CACHE; do
	if [ -f $d ]; then
		dvc_error "Dummy cache file $d was not removed by gc"
		dvc_fail
	fi
done

dvc_pass
