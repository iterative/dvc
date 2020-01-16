#!/bin/bash

set -e

if [[ $TRAVIS_OS_NAME == "osx" && $TRAVIS_OSX_IMAGE != "xcode8.3" ]]; then
    exit 1
fi

if [[ $TRAVIS_EVENT_TYPE = pull_request || $TRAVIS_EVENT_TPYE = cron ]]; then
    exit 2
fi

# ensure at least one positional arg exists
if [[ ${#} -ge 1 ]]; then
  [[ -n "$(ls $@ 2>/dev/null)" ]] || exit 3
fi

echo true
