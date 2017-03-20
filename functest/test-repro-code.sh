#!/bin/bash

source common.rc

function run_test() {
    mkdir data/tsv
    dvc-run --code code/xmltotsv.py python code/xmltotsv.py --extract row/@Id,row/@UserId,row/@Name,row/@Class,row/@TagBased,row/@Date data/xml/Badges.xml data/tsv/Badges.tsv
    
    echo " " >> code/xmltotsv.py 
    git commit -am 'Change code'
    dvc-repro data/tsv/Badges.tsv
    
}

TEST_REPO=repo_repro_test
create_repo
(cd repo_repro_test; run_test)
