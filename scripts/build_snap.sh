#!/bin/bash
set -e
set -x

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root"
  exit 1
fi

sudo snapcraft --use-lxd

pip uninstall -y dvc
if which dvc; then
  echo "ERROR: dvc command still exists! Unable to verify dvc version." >&2
  exit 1
fi
sudo snap install --dangerous --classic dvc_*.snap
if [[ -n "$TRAVIS_TAG" ]]; then
  # Make sure we have a correct version
  if [[ "$(dvc --version)" != "$TRAVIS_TAG" ]]; then
      echo "ERROR: 'dvc --version'$(dvc -V) doesn't match '$TRAVIS_TAG'" >&2
      exit 1
  fi
fi
# ensure basic commands can run
dvc version
dvc get -v https://github.com/iterative/dvc scripts/innosetup/dvc.ico
sudo snap remove dvc
