#!/bin/bash

set -e

source common.sh

ITEM=data/xml/Badges.xml

dvc_create_repo

rm -f $ITEM
dvc checkout
dvc_check_files $ITEM
dvc_pass
