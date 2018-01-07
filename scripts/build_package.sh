#!/bin/bash

set -e

if [ ! -d "dvc" ]; then
	echo "Please run this script from repository root"
	exit 1
fi

python setup.py sdist
python setup.py bdist_wheel --universal
