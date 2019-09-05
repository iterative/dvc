#!/bin/bash

test_description='Make sure we are running the dev version of dvc'
source "$(dirname "$0")"/init.sh

test_expect_success 'Check `dvc version`' '
    local rev=$(git log | head -n 1 | cut -d" " -f2 | cut -c 1-6) &&
    dvc version | grep "^DVC version:" | grep "$rev.mod"
'

test_done
