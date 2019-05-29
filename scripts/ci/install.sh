#!/bin/bash

set -x
set -e

N_RETRIES=3

function retry {
    for i in $(seq $N_RETRIES); do
        $@ && break
    done
}

# NOTE: it is not uncommon for pip to hang on travis for what seems to be
# networking issues. Thus, let's retry a few times to see if it will eventially
# work or not.
retry pip install --upgrade pip setuptools wheel
retry pip install -e .[all]
retry pip install -e .[tests]

git config --global user.email "dvctester@example.com"
git config --global user.name "DVC Tester"

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
	aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
	aws configure set region us-east-2

	openssl enc -d -aes-256-cbc -md md5 -k $GCP_CREDS -in scripts/ci/gcp-creds.json.enc -out scripts/ci/gcp-creds.json
fi
