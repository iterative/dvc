#!/bin/bash

set -x
set -e

# stop the build if there are any formatting errors
GO111MODULE=on pre-commit run --all-files shfmt
