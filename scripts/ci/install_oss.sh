#!/bin/bash

set -euo pipefail

git clone https://github.com/iterative/oss-emulator.git
sudo docker image build -t oss:1.0 oss-emulator
sudo docker run --detach --restart always -p 8880:8880 --name oss-emulator oss:1.0
echo "export OSS_ENDPOINT='localhost:8880'" >> env.sh
echo "export OSS_ACCESS_KEY_ID='AccessKeyID'" >> env.sh
echo "export OSS_ACCESS_KEY_SECRET='AccessKeySecret'" >> env.sh
