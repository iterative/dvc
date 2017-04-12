#!/bin/bash

source common.rc

function run_test() {
    mkdir data/tsv

    dvc_info 'convert Budget.xml to tsv'
    dvc run --code code/xmltotsv.py python code/xmltotsv.py --extract row/@Id,row/@UserId,row/@Name,row/@Class,row/@TagBased,row/@Date data/xml/Badges.xml data/tsv/Badges.tsv
    
    dvc_info 'modify code'
    echo " " >> code/xmltotsv.py 
    git commit -am 'Change code'
    
    dvc_info 'reproduce tsv convertion code'
    dvc repro data/tsv/Badges.tsv
}

create_repo
(cd $TEST_REPO; run_test)
