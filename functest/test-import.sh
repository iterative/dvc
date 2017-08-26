#!/bin/bash

set -e

source common.sh

dvc_create_repo

dvc_info "Import from local file"
dvc import $DATA_CACHE/Tags.xml data/local
dvc_check_files "data/local"

dvc_info "Import from http url"
dvc import $DATA_S3/Tags.xml data/http
dvc_check_files "data/http"

if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
	dvc_info "Import from s3 url"
	dvc config aws.region $REGION
	dvc import s3://dataversioncontrol/functests/stackoverflow_raw_small/Tags.xml data/s3
	dvc_check_files "data/s3"
fi

dvc_pass
