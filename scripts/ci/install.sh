#!/bin/bash

set -x
set -e

scriptdir="$(dirname $0)"

# NOTE: it is not uncommon for pip to hang on travis for what seems to be
# networking issues. Thus, let's retry a few times to see if it will eventually
# work or not.
$scriptdir/retry.sh pip install .[all,tests]

git config --global user.email "dvctester@example.com"
git config --global user.name "DVC Tester"

if [[ "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
  aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
  aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
  aws configure set region us-west-2

  openssl enc -d -aes-256-cbc -md md5 -k $GCP_CREDS -in scripts/ci/gcp-creds.json.enc -out scripts/ci/gcp-creds.json
fi
