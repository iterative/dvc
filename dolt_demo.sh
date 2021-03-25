#!/bin/bash

set -xeou pipefail

DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
BASE=$DIR
TMP=$BASE/tmp

rm -rf $TMP \
    && mkdir -p $TMP

cd $TMP

# make the dolt directories
mkdir -p dolt_source \
    && cd dolt_source \
    && dolt init \
    && dolt sql -q "create table t1 (a int primary key, b int)" \
    && dolt sql -q "insert into t1 values (0,0), (1,1)" \
    && dolt commit -am "Initialize source table" \
    && cd $TMP

mkdir -p dolt_target \
    && cd dolt_target \
    && dolt init \
    && dolt sql -q "create table t2 (a int primary key, b int)" \
    && dolt commit -am "Initialize target table" \
    && cd $TMP

# add the input data source
git init
dvc init
dvc add dolt_source
echo "dolt metadata in dolt_source.dvc:"
cat dolt_source.dvc

# change and then checkout original source
cd dolt_source \
    && dolt sql -q "drop table t1" \
    && dolt sql -q "show tables" \
    && cd $TMP

dvc checkout dolt_source \
    && cd dolt_source \
    && dolt sql -q "show tables" \
    && cd $TMP

# remotes with dolt
UPSTREAM=$TMP/upstream
mkdir -p $UPSTREAM
dvc remote add upstream $UPSTREAM \
    && dvc push -r upstream dolt_source \
    && rm -rf dolt_source
    #&& ls -al dolt_source 2> /dev/null

dvc pull -r upstream dolt_source \
    && ls -al dolt_source/.dolt

#run with dolt source & target
$SCRIPT=$TMP/script.py
cat <<EOF > $SCRIPT
import sys
import doltcli as dolt
_, source, target = sys.argv

print(source, target)
source_db = dolt.Dolt(source)
target_db = dolt.Dolt(target)
rows = source_db.sql("select * from t1", result_format="csv")
dolt.write_rows(target_db, "t2", rows, commit=True, commit_message="Automated row-add")
EOF

dvc add $SCRIPT

dvc run -n dolt_single_stage \
    -d $SCRIPT \
    -d dolt_source \
    -o dolt_target \
    python $SCRIPT dolt_source dolt_target

echo "dolt single-stage pipeline"
cat dolt_target.dvc

cd dolt_target \
    && dolt -r csv -q "select * from t2" \
    && cd $TMP
