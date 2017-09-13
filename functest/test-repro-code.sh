#!/bin/bash

set -e

source common.sh

dvc_create_repo

mkdir data/tsv

dvc_info 'Convert Budget.xml to tsv'
dvc run --code code/xmltotsv.py python code/xmltotsv.py --extract row/@Id,row/@UserId,row/@Name,row/@Class,row/@TagBased,row/@Date data/xml/Badges.xml data/tsv/Badges.tsv
    
dvc_info 'Modify code'
echo " " >> code/xmltotsv.py 
git commit -am 'Change code'
    
dvc_info 'Reproduce tsv convertion code'
dvc repro data/tsv/Badges.tsv

dvc_pass
