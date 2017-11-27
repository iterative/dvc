#!/bin/bash

set -e

source common.sh

TESTS+=" test-init.sh"
TESTS+=" test-add.sh"
TESTS+=" test-sync.sh"
TESTS+=" test-repro-code.sh"
TESTS+=" test-merge.sh"
TESTS+=" test-checkout.sh"
TESTS+=" test-gc.sh"

for t in $TESTS; do
	rm -rf $TEST_REPO
	dvc_header "$t"
	./$t
done

rm -rf $TEST_REPO
