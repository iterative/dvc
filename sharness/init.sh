# This file should be sourced by all test scripts

source .env/bin/activate

: "${SHARNESS_TEST_SRCDIR:=./lib/sharness}"
source "$SHARNESS_TEST_SRCDIR/sharness.sh"


