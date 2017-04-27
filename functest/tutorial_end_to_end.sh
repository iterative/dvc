
# 1. First ML model
dvc init
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/
dvc run tar zxf data/Posts.xml.tgz -C data/

mkdir code
wget -nv -P code/ https://s3-us-west-2.amazonaws.com/dvc-share/so/code/df_to_matrix.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/metrics.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/train_model.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/train_test_split.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/xml_to_tsv.py
git add code/
git commit -m 'Download code'

dvc run python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
dvc run python code/train_test_split.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
dvc run python code/df_to_matrix.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p

dvc run python code/train_model.py data/matrix-train.p data/model.p

dvc run python code/metrics.py data/model.p  data/matrix-test.p data/summary.txt

cat data/summary.txt
# AUC: 0.552980

# 2. Reproduce: change input dataset

dvc remove data/Posts.xml.tgz
#dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/100K/Posts.xml.tgz data/
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/
dvc repro data/summary.txt
cat data/summary.txt
# AUC: 0.639861

# 3. Share your research

# Improve features
echo " " >> code/df_to_matrix.py
git add code/df_to_matrix.py
git commit -m 'Include bigram'
dvc repro data/summary.txt
cat data/summary.txt
#

#dvc repro data/Posts-train.tsv -f
#dvc repro data/summary.txt
#cat data/summary.txt
