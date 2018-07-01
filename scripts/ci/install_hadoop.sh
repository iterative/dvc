#!/bin/bash

# NOTE: based on http://sharafjaffri.blogspot.com/2015/04/installing-single-node-hadoop-26-using.html

set -x
set -e

# This command is used to tell shell, turn installation mode to non interactive
# and set auto selection of agreement for Sun Java
export DEBIAN_FRONTEND=noninteractive
echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections

sudo apt-get update -y
sudo apt-get install python-software-properties -y
sudo add-apt-repository ppa:webupd8team/java -y
sudo apt-get update -y
sudo -E apt-get install oracle-java8-installer -y

java -version

pushd /usr/local
sudo wget https://s3-us-west-2.amazonaws.com/dvc-share/test/hadoop-2.6.5.tar.gz
sudo tar xzf hadoop-2.6.5.tar.gz
sudo mkdir hadoop
sudo mv hadoop-2.6.5/* hadoop/
popd

echo "export HADOOP_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_MAPRED_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_COMMON_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_HDFS_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export YARN_HOME=/usr/local/hadoop" >> ~/.bashrc
echo "export HADOOP_COMMON_LIB_NATIVE_DIR=/usr/local/hadoop/lib/native" >> ~/.bashrc
echo "export JAVA_HOME=/usr/" >> ~/.bashrc
echo "export PATH=$PATH:/usr/local/hadoop/sbin:/usr/local/hadoop/bin:$JAVA_PATH/bin" >> ~/.bashrc

cat ~/.bashrc

source ~/.bashrc

sudo bash -c 'echo "export JAVA_HOME=/usr/" >> /usr/local/hadoop/etc/hadoop/hadoop-env.sh'

sudo cp scripts/ci/core-site.xml /usr/local/hadoop/etc/hadoop
sudo cp scripts/ci/hdfs-site.xml /usr/local/hadoop/etc/hadoop
