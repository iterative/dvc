#!/bin/bash

set -e

source common.sh

function test_sync() {
	dvc_info "Checking status"
	dvc status data/xml

	dvc_info "Pushing data"
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

	dvc_info "Removing all cache"
	rm -rf .cache/xml

	dvc_info "Checking status"
	dvc status data/xml

	dvc_info "Pulling data"
	dvc pull data/xml
} 

function test_aws() {
	dvc_create_repo

	dvc_info "Setting up AWS cloud"
	dvc_clean_cloud_aws
	dvc config global.cloud AWS
	dvc config aws.storagepath $TEST_REPO_S3
	dvc config aws.region $TEST_REPO_REGION

	test_sync

	dvc_clean_cloud_aws
}

function test_gcp() {
	dvc_create_repo

	dvc_info "Setting up GCP cloud"
	dvc_clean_cloud_gcp
	dvc config global.cloud GCP
	dvc config gcp.storagepath $TEST_REPO_GCP
	dvc config gcp.projectname $TEST_REPO_GCP_PROJECT

	test_sync

	dvc_clean_cloud_gcp
}

if [[ "$TRAVIS_PULL_REQUEST" == "false" && "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	test_aws
	# Temporarily disabled for travis
	if [[ "$TRAVIS" != "true" ]]; then
		test_gcp
	fi
fi

dvc_pass
