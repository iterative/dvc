#!/usr/bin/env bash

set -euo pipefail

# install docker
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install -y docker-ce

# run azurite
sudo docker run --restart always -e executable=blob -p 10000:10000 --tmpfs /opt/azurite/folder -d arafato/azurite:2.6.5

# save secrets
echo "export AZURE_STORAGE_CONTAINER_NAME='travis-tests'" >> env.sh
echo "export AZURE_STORAGE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'" >> env.sh
