#!/bin/bash

set -e

N_RETRIES=3
for i in $(seq $N_RETRIES); do
    echo "Attempt #$i"
    $@ && exit $?
done

exit $?
