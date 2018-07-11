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
ssh-keyscan localhost >> ~/.ssh/known_hosts
ssh-keyscan 127.0.0.1 >> ~/.ssh/known_hosts
ssh-keyscan 0.0.0.0 >> ~/.ssh/known_hosts
ssh 0.0.0.0 ls &> /dev/null
ssh 127.0.0.1 ls &> /dev/null
ssh localhost ls &> /dev/null

scriptdir="$(dirname $0)"

if [ -n "$TRAVIS_OS_NAME" ] && [ "$TRAVIS_OS_NAME" != "osx" ]; then
  bash "$scriptdir/install_azurite.sh"
  source ~/.bashrc
fi
