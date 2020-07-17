#!/bin/bash

set -x
set -e

scriptdir="$(dirname $0)"

if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
  ulimit -a
  sudo sysctl -w kern.maxproc=2048
  sudo sysctl -w kern.maxprocperuid=2048
  echo '\nulimit -u 2048' >>~/.bash_profile
  ulimit -a
fi

echo >env.sh

if [[ "$TRAVIS_BUILD_STAGE_NAME" == "test" ]]; then
  if [[ "$TRAVIS_OS_NAME" == "osx" && "$TRAVIS_PULL_REQUEST" == "false" ]]; then
    brew install openssl
    $scriptdir/retry.sh brew cask install google-cloud-sdk
  fi
fi

if [[ "$TRAVIS_OS_NAME" == "windows" ]]; then
  $scriptdir/retry.sh choco install python --version $PYTHON_VERSION
  echo "PATH='$PATH'" >>env.sh
elif [[ "$TRAVIS_OS_NAME" == "osx" ]]; then
  ln -s -f /usr/local/bin/python3 /usr/local/bin/python
  ln -s -f /usr/local/bin/pip3 /usr/local/bin/pip
fi

if [ "$TRAVIS_OS_NAME" == "linux" ]; then
  # fetch tags for `git-describe`, since
  # - can't rely on $TRAVIS_TAG for snapcraft `edge` (master) releases, and
  # - `snapcraft` also uses `git-describe` for version detection
  git fetch --tags
  TAG_MAJOR="$(git describe --tags | sed -r 's/^v?([0-9]+)\.[0-9]+\.[0-9]+.*/\1/')"
  [[ -n "$TAG_MAJOR" ]] || exit 1  # failed to detect major version

  if [[ -n "$TRAVIS_TAG" ]]; then
    if [[ $(echo "$TRAVIS_TAG" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$') ]]; then
      SNAP_CHANNEL=stable
    else
      SNAP_CHANNEL=beta
    fi
  else
    SNAP_CHANNEL=edge
  fi
  echo "export SNAP_CHANNEL=${SNAP_CHANNEL},v${TAG_MAJOR}/${SNAP_CHANNEL}" >>env.sh
  # NOTE: after deprecating this major version branch, uncomment this line
  # echo "export SNAP_CHANNEL=v${TAG_MAJOR}/${SNAP_CHANNEL}" >>env.sh
fi
