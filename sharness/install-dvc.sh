#!/bin/bash

cd "$(dirname "$0")"

virtualenv --quiet --python python3 .env
source .env/bin/activate
#pip3 install --quiet --editable "..[all]"
pip3 install --quiet --editable ".."

echo -e "\n=====================================================\n"
dvc version
echo -e "=====================================================\n"
