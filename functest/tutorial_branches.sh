#!/bin/bash

set -e

# 1. First ML model

mkdir myrepo
cd myrepo

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
#pip install -U -r code/requirements.txt
# Edit eval file format
git add code/
git commit -m 'Download code'

dvc init

git checkout -b input_100K
mkdir data
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/100K/Posts.xml.tgz data/
dvc run tar zxf data/Posts.xml.tgz -C data/
dvc run python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python


git checkout master
dvc checkout
git checkout -b input_25K
dvc checkout
mkdir data
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/
dvc run tar zxf data/Posts.xml.tgz -C data/

dvc run python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
dvc run python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
dvc run python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p

dvc run python code/train_model.py data/matrix-train.p 20170426 data/model.p

dvc run python code/evaluate.py data/model.p data/matrix-test.p data/eval_auc.txt

dvc config Global.Target data/eval_auc.txt
git commit -am 'Set Target'

cat data/eval_auc.txt
# AUC: 0.596182


git checkout input_100K
dvc checkout
git merge -X theirs input_25K # git merge -X theirs input_25K
# Resolve conflicts by hends!!!
git add .
git commit -m 'Merge conflicts'
dvc merge
dvc repro       # <-- Error. Reproducng Posts.xml and Posts.tsv


# Improve the model:
git checkout input_25K
dvc checkout
vi code/train_model.py  # Change: estimators=500
git commit -am 'estimators=500'
vi code/train_model.py  # Change: n_jobs=6
git commit -am 'n_jobs=6'
dvc repro
cat data/eval_auc.txt
# 0.619262
vi code/featurization.py # Change: add ngram_range=(1, 2) to CountVectorizer
git commit -am 'Add bigrams'
dvc repro
cat data/eval_auc.txt
# 0.628989
vi code/featurization.py
git commit -am 'Add three-grams' # Change: ngram_range=(1, 3)
dvc repro
cat data/eval_auc.txt
# 0.630682
vi code/featurization.py
git commit -am 'Add 4-grams'    # Change: ngram_range=(1, 4)
dvc repro
cat data/eval_auc.txt
# 0.621002

# Create a branch for unsuccessful commit
git checkout -b four_grams
dvc checkout
# The last successful commit which is the second last humman-created commit
git log --pretty=oneline | grep -v "DVC repro-run: " | head -n 2 | tail -n 1
# Move the current HEAD to the last successful commit
git branch -f input_25K `git log --pretty=oneline | grep -v "DVC repro-run: " | head -n 2 | tail -n 1 | cut -d" " -f1`
git checkout input_25K
dvc checkout

# Continue to optimize the target metrics
vi code/train_model.py  # Change: min_samples_split=5
git commit -am 'min_samples_split=5'
dvc repro

git checkout input_100K
dvc checkout
git merge -X theirs input_25K
git add data/
git commit -am "After merge"
dvc repro

##

vi code/featurization.py
# 0.578447

# 2. Reproduce: change input dataset

dvc remove data/Posts.xml.tgz
#dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/100K/Posts.xml.tgz data/
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/
dvc repro data/evaluation.txt
cat data/evaluation.txt
# AUC: 0.639861

# 3. Share your research

# Improve features
echo " " >> code/featurization.py
git add code/featurization.py
git commit -m 'Include bigram'
dvc repro data/evaluation.txt
cat data/evaluation.txt
