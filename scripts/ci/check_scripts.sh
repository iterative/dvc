#!/bin/bash

set -x
set -e

GO111MODULE=on go get mvdan.cc/sh/v3/cmd/shfmt

# stop the build if there are any formatting errors
shfmt -l -d -i 2 -ci -w .
