#!/bin/bash

set -e

# 1. First ML model

rm -rf myrepo
mkdir myrepo
cd myrepo
mkdir data
mkdir code
wget -nv -P code/ https://s3-us-west-2.amazonaws.com/dvc-share/so/code/featurization.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/evaluate.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/train_model.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/split_train_test.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/xml_to_tsv.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/requirements.txt
pip install -U -r code/requirements.txt

git init
git add code/
git commit -m 'Download code'


dvc init
wget -P data/ https://s3-us-west-2.amazonaws.com/dvc-share/so/10K/Posts.xml.tgz
dvc add data/Posts.xml.tgz
dvc run -d data/Posts.xml.tgz -o data/Posts.xml tar zxf data/Posts.xml.tgz -C data/

dvc run -d code/xml_to_tsv.py -d data/Posts.xml -o data/Posts.tsv python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
dvc run -d code/split_train_test.py -d data/Posts.tsv -o data/Posts-test.tsv python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
dvc run -d code/featurization.py -d data/Posts-train.tsv -d data/Posts-test.tsv -o data/matrix-train.p -o data/matrix-test.p python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p

dvc run -d code/train_model.py -d data/matrix-train.p -o data/model.p python code/train_model.py data/matrix-train.p 20170426 data/model.p

dvc run -d code/evaluate.py -d data/model.p -d data/matrix-test.p -o data/evaluation.txt python code/evaluate.py data/model.p data/matrix-test.p data/evaluation.txt

cat data/evaluation.txt
# AUC: 0.552980

exit 0

# 2. Reproduce: change input dataset

dvc remove data/Posts.xml.tgz
#dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/100K/Posts.xml.tgz data/

wget -P data/ https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz
dvc add data/Posts.xml.tgz
dvc repro -v data/evaluation.txt
cat data/evaluation.txt
# AUC: 0.639861

# 3. Share your research

# Improve features
echo " " >> code/featurization.py
git add code/featurization.py
git commit -m 'Include bigram'
dvc repro data/evaluation.txt
cat data/evaluation.txt
