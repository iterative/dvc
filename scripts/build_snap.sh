#!/bin/bash
set -e
set -x

if [ ! -d "dvc" ]; then
  echo "Please run this script from repository root"
  exit 1
fi

sg lxd -c snapcraft
original_snap_name="$(ls dvc_*.snap)"
mv "$original_snap_name" original.snap

git apply scripts/remove_update_warning.patch
sg lxd -c snapcraft
mv dvc_*.snap nowarn_update_"$original_snap_name".snap
mv original.snap "$original_snap_name"

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
# N.B.: cannot run `dvc get` on travis (#2956)
dvc version
dvc.git status
dvc.git fetch --all
sudo snap remove dvc
