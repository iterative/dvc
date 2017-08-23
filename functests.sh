#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BIN=$DIR/bin
DVC_HOME=$DIR

export PATH=$BIN:$PATH
export DVC_HOME=$DIR

cd functest
./run.sh
