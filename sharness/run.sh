#!/bin/bash

BLUE='\033[0;34m'
NOCOLOR='\033[0m'

opts=''
if [[ $1 == '-d' || $1 == '--debug' ]]; then
    opts='--verbose'
    shift
fi

echo -e "\n${BLUE}=> install sharness${NOCOLOR}"
lib/install-sharness.sh
echo -e "\n${BLUE}=> install dvc${NOCOLOR}"
./install-dvc.sh
echo -e "\n${BLUE}=> clean any existing test results${NOCOLOR}"
rm -rf test-results

pattern=${@:-*.t}
set -e
cd "$(dirname "$0")"
for t in $(ls $pattern); do
    [[ ${t: -2} == ".t" ]] || continue
    [[ -x $t ]] || continue
    echo -e "\n${BLUE}=> ./$t${NOCOLOR}"
    ./$t $opts
done

echo -e "\n${BLUE}=> aggregate test results${NOCOLOR}"
ls test-results/t*-*.*.counts | lib/sharness/aggregate-results.sh

#prove $pattern

# Example:
#     tests/run.sh
#     tests/run.sh t0000-sharness.t t0001-dvc.t
#     tests/run.sh t0*
#     tests/run.sh *-add-*

