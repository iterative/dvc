#!/bin/bash

set -e

python setup.py sdist
python setup.py bdist_wheel --universal
