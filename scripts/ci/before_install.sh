#!/bin/bash

set -x
set -e

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew update
    brew upgrade python
    python --version
fi

pip install -r scripts/ci/requirements.txt

# stop the build if there are any readme formatting errors
python setup.py checkdocs

# stop the build if there are any black errors
black dvc --check
black tests --check

#NOTE: ssh keys for ssh test to be able to ssh to the localhost
ls -la ~/.ssh/

ssh-keygen -t rsa -N "" -f mykey
mkdir -p ~/.ssh
cp mykey ~/.ssh/id_rsa
cp mykey.pub ~/.ssh/id_rsa.pub
cat mykey.pub >> ~/.ssh/authorized_keys
ssh-keyscan localhost >> ~/.ssh/known_hosts
ssh-keyscan 127.0.0.1 >> ~/.ssh/known_hosts
ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts
ssh 0.0.0.0 ls &> /dev/null
ssh 127.0.0.1 ls &> /dev/null
ssh localhost ls &> /dev/null

scriptdir="$(dirname $0)"

echo > env.sh
if [ -n "$TRAVIS_OS_NAME" ] && [ "$TRAVIS_OS_NAME" != "osx" ] \
   && [ "$TRAVIS_EVENT_TYPE" == "cron" ]; then
  bash "$scriptdir/install_azurite.sh"
  bash "$scriptdir/install_hadoop.sh"
fi

if  [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
    brew install openssl
    brew cask install google-cloud-sdk
fi
