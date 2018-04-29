#!/bin/bash

set -x
set -e

pip install --upgrade pip
pip install -r requirements.txt
pip install -r test-requirements.txt
git config --global user.email "dvctester@example.com"
git config --global user.name "DVC Tester"

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
	aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
	aws configure set region us-east-2

	openssl enc -d -aes-256-cbc -md md5 -k $GCP_CREDS -in scripts/ci/gcp-creds.json.enc -out scripts/ci/gcp-creds.json
fi
