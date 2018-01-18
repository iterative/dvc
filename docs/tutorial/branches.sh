#!/bin/bash

set -e

mkdir myrepo
cd myrepo

# 1. Set up environment and get code
git init
echo "This is an empty readme" > README.md
git add README.md
git commit -m 'add readme'

mkdir code
wget -nv -P code/ https://s3-us-west-2.amazonaws.com/dvc-share/so/code/featurization.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/evaluate.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/train_model.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/split_train_test.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/xml_to_tsv.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/requirements.txt
pip install -U -r code/requirements.txt
git add code/
git commit -m 'Download code'
dvc init
git commit -m 'Init DVC'

# 2. First steps in 100K dataset
git checkout -b input_100K
mkdir data
wget https://s3-us-west-2.amazonaws.com/dvc-share/so/100K/Posts.xml.tgz -O data/Posts.xml.tgz
#cp ../stackoverflow_small_xml/100K/Posts.xml.tgz data/
dvc add data/Posts.xml.tgz
dvc run -o data/Posts.xml -d data/Posts.xml.tgz tar zxf data/Posts.xml.tgz -C data/
dvc run -d code/xml_to_tsv.py -d data/Posts.xml -o data/Posts.tsv python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
git add .
git commit -m 'Extract TSV for 100K dataset'

# 3. Define full pipeline for 25K dataset
git checkout master
dvc checkout
git checkout -b input_25K
dvc checkout
mkdir data
wget https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz -O data/Posts.xml.tgz
#cp ../stackoverflow_small_xml/25K/Posts.xml.tgz data/
dvc add data/Posts.xml.tgz
dvc run -o data/Posts.xml -d data/Posts.xml.tgz tar zxf data/Posts.xml.tgz -C data/
dvc run -d code/xml_to_tsv.py -d data/Posts.xml -o data/Posts.tsv python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python

dvc run -d code/split_train_test.py -d data/Posts.tsv -o data/Posts-train.tsv -o data/Posts-test.tsv python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
dvc run -d code/featurization.py -d data/Posts-train.tsv -d data/Posts-test.tsv -o data/matrix-train.p -o data/matrix-test.p python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p
dvc run -d code/train_model.py -d data/matrix-train.p -o data/model.p python code/train_model.py data/matrix-train.p 20170426 data/model.p
dvc run -d code/evaluate.py -d data/model.p -d data/matrix-test.p -O data/eval_auc.txt python code/evaluate.py data/model.p data/matrix-test.p data/eval_auc.txt
dvc run -d data/eval_auc.txt
cat data/eval_auc.txt
# AUC: 0.596182

git status
git add .
git commit -m 'Full pipeline'

# 4. Integrate the 25K dataset pipeline into 100K branch
git checkout input_100K
dvc checkout
git merge -X ours input_25K # You can also merge and resolve conflicts by hands!!!

dvc repro
git add .
git commit -m 'Repro'

# 5. Improve the 25K model
git checkout input_25K
dvc checkout
vi code/train_model.py  # Change: estimators=500 and n_jobs=6
dvc repro
git commit -am 'estimators=500'
cat data/eval_auc.txt
# 0.619262

vi code/featurization.py # Change: add ngram_range=(1, 2) to CountVectorizer
dvc repro
git commit -am 'Add bigrams'
cat data/eval_auc.txt
# 0.628989

vi code/featurization.py # Change: ngram_range=(1, 3)
dvc repro
git commit -am 'Add three-grams'
cat data/eval_auc.txt
# 0.630682

vi code/featurization.py  # Change: ngram_range=(1, 4)
dvc repro
git commit -am 'Add 4-grams'
cat data/eval_auc.txt
# 0.621002

# 6. Revert the last step back
# Create a branch for unsuccessful commit
git checkout -b four_grams
dvc checkout
# The last successful commit which is the second last humman-created commit
git log --pretty=oneline | grep -v "DVC repro-run: " | head -n 2 | tail -n 1
# Move the current HEAD to the last successful commit
git branch -f input_25K `git log --pretty=oneline | grep -v "DVC repro-run: " | head -n 2 | tail -n 1 | cut -d" " -f1`
git checkout input_25K
dvc checkout

# 7. Integrate the 25K model improvements into the 100K model
git checkout input_100K
dvc checkout
git merge -X theirs input_25K
dvc repro
git commit -am 'Final model for 100K dataset'
cat data/eval_auc.txt
# 0.652154

