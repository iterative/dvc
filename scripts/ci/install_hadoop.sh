#!/bin/bash

# NOTE: based on http://sharafjaffri.blogspot.com/2015/04/installing-single-node-hadoop-26-using.html

set -x
set -e

sudo apt-get update -y
sudo apt-get install openjdk-8-jdk
java -version

pushd /usr/local
sudo wget https://s3-us-west-2.amazonaws.com/dvc-share/test/hadoop-2.6.5.tar.gz
sudo tar xzf hadoop-2.6.5.tar.gz
sudo mkdir hadoop
sudo mv hadoop-2.6.5/* hadoop/
popd

echo "export HADOOP_HOME=/usr/local/hadoop" >> env.sh
echo "export HADOOP_MAPRED_HOME=/usr/local/hadoop" >> env.sh
echo "export HADOOP_COMMON_HOME=/usr/local/hadoop" >> env.sh
echo "export HADOOP_HDFS_HOME=/usr/local/hadoop" >> env.sh
echo "export YARN_HOME=/usr/local/hadoop" >> env.sh
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/local/hadoop/lib/native" >> env.sh
echo "export JAVA_HOME=/usr/" >> env.sh
echo "export PATH=\$PATH:/usr/local/hadoop/sbin:/usr/local/hadoop/bin:$JAVA_PATH/bin" >> env.sh

source env.sh

sudo bash -c 'echo "export JAVA_HOME=/usr/" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh'

sudo cp scripts/ci/core-site.xml /usr/local/hadoop/etc/hadoop
sudo cp scripts/ci/hdfs-site.xml /usr/local/hadoop/etc/hadoop

hadoop fs -help
pushd ~
sudo mkdir -p /home/hadoop/hadoopinfra/hdfs/namenode/
sudo chmod 777 /home/hadoop/hadoopinfra/hdfs/namenode/
sudo chmod 777 -R /usr/local/hadoop
hdfs namenode -format
start-dfs.sh
jps
hadoop fs -ls /
hadoop fs -ls hdfs://127.0.0.1/
hadoop fs -mkdir hdfs://127.0.0.1/ololo
popd
