#!/bin/bash

set -e

source common.sh

if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	dvc_create_repo

	dvc_info "Setting up AWS cloud"
	dvc_clean_cloud_aws
	dvc config aws.storagepath $TEST_REPO_S3
	dvc config aws.region $TEST_REPO_REGION

	dvc_info "Checking status"
	dvc status data/xml

	dvc_info "Pushing to AWS"
	dvc push data/xml

	dvc_info "Checking status"
	dvc status data/xml

	dvc_info "Modifying data"
	echo "123456" >> data/xml/Tags.xml

	dvc_info "Checking status"
	dvc status data/xml

	dvc_info "Pulling data"
	dvc pull data/xml

	dvc_info "Checking status"
	dvc status data/xml

	dvc_clean_cloud_aws
fi

dvc_pass
