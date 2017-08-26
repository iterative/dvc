#!/bin/bash

set -e

SDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CACHE_DIR=$SDIR/cache
TEST_REPO=$SDIR/myrepo
CODE_CACHE=$CACHE_DIR/code
DATA_CACHE=$CACHE_DIR/data

DATA="Badges.xml Tags.xml PostLinks.xml"
CODE="xmltotsv.py"

REGION='us-west-2'
BASE_DIR_S3=https://s3-$REGION.amazonaws.com/dataversioncontrol/functests
DATA_S3=$BASE_DIR_S3/stackoverflow_raw_small
CODE_S3=$BASE_DIR_S3/code

TEST_REPO_S3='dvc-test/myrepo'
TEST_REPO_REGION='us-east-2'

function dvc_print() {
	echo -e "$1"
}

function dvc_info() {
        dvc_print "\e[34m$1\e[0m"
}

function dvc_header() {
	dvc_info "=============== DVC test: $1 ==============="
}

function dvc_error() {
	dvc_print "\e[31m$1\e[0m"
}

function dvc_fail() {
	dvc_error "FAIL"
	exit 1
}

function dvc_fatal() {
	dvc_error "$1"
	dvc_fail
}

function dvc_pass() {
	dvc_print "\e[32mPASS\e[0m"
	exit 0
}

trap 'dvc_fail;' ERR

function dvc_prepare_cache() {
	if [ -d $CACHE_DIR ]; then
		return
	fi

	mkdir -p $DATA_CACHE
	pushd $DATA_CACHE

	for f in $DATA; do
		wget $DATA_S3/$f
	done

	popd

	mkdir -p $CODE_CACHE
	pushd $CODE_CACHE

	for f in $CODE; do
		wget $CODE_S3/$f
	done

	popd
}

function dvc_create_git_repo() {
	dvc_info "Creating git repo"

	dvc_prepare_cache

	mkdir $TEST_REPO
        cd $TEST_REPO

	git init

        git config user.name "DVC tester"
	git config user.email "dvctester@dataversioncontrol.com"

	mkdir code
	cp $CODE_CACHE/* code/
	git add code/
	git commit -m 'Add code'
}

function dvc_create_repo() {
	dvc_info "Creating dvc repo"

	dvc_create_git_repo

	dvc init

	mkdir data/xml
	dvc import $DATA_CACHE/* data/xml
}

function dvc_clean_cloud_aws() {
	dvc_info "Cleaning $TEST_REPO_S3"
	aws s3 rm --recursive "s3://$TEST_REPO_S3/"
}

function dvc_check_dirs() {
	for d in $1; do
		if [ ! -d "$d" ]; then
			dvc_fatal "Directory $d doesn't exist"
		fi
	done
}

function dvc_check_files() {
	for f in $1; do
		if [ ! -f "$f" ]; then
			dvc_fatal "File $f doesn't exist"
		fi
	done
}
