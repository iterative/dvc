#!/bin/bash

set -x
set -e

pip install --upgrade pip
pip install -r requirements.txt
pip install -r test-requirements.txt
git config --global user.email "dvctester@example.com"
git config --global user.name "DVC Tester"
mkdir ~/.aws
printf "[default]\n" > ~/.aws/credentials
printf "aws_access_key_id = $AWS_ACCESS_KEY_ID\n" >> ~/.aws/credentials
printf "aws_secret_access_key = $AWS_SECRET_ACCESS_KEY\n" >> ~/.aws/credentials
printf "[default]\n" > ~/.aws/config
openssl enc -d -aes-256-cbc -md md5 -k $GCP_CREDS -in scripts/ci/gcp-creds.json.enc -out scripts/ci/gcp-creds.json || true
