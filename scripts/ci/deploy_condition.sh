#!/bin/bash

set -e

if [[ $TRAVIS_EVENT_TYPE = pull_request || $TRAVIS_EVENT_TPYE = cron ]]; then
  exit 2
fi

# positional args are assumed to be file glob patterns to deploy
if [[ ${#} -ge 1 ]]; then
  # ensure at least one file exists
  [[ -n "$(ls $@ 2>/dev/null)" ]] || exit 3
fi

echo true
