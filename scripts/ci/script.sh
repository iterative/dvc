#!/bin/bash

set -x
set -e

#NOTE: ssh keys for ssh test to be able to ssh to the localhost
ls -la ~/.ssh/

ssh-keygen -t rsa -N "" -f mykey
mkdir -p ~/.ssh
cp mykey ~/.ssh/id_rsa
cp mykey.pub ~/.ssh/id_rsa.pub
cat mykey.pub >> ~/.ssh/authorized_keys
ssh-keyscan 127.0.0.1 >> ~/.ssh/known_hosts
ssh 127.0.0.1 ls &> /dev/null

python -mtests

if [[ "$TRAVIS_PULL_REQUEST" == "false" && \
      "$TRAVIS_SECURE_ENV_VARS" == "true" ]]; then
	codecov
fi

if [[ ! -z "$TRAVIS_TAG" ]]; then
	./scripts/build_package.sh
	./scripts/build_posix.sh
fi
