#!/bin/bash

set -e

source common.sh

dvc_create_repo

# Adding changes to the first branch
git checkout -b input_50K
echo -e "#!/bin/bash\nset -x\nset -e\ncp \$1 \$2" > code/code1.sh
chmod +x code/code1.sh
cp code/code1.sh code/code2.sh
cp code/code1.sh code/code3.sh
cp code/code1.sh code/code4.sh
git add code
git commit -s -m"add code"

dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/50K/Posts.xml.tgz data/data

ORIG_DATA=$(dvc_md5 data/data)

dvc run ./code/code1.sh data/data data/data1
dvc run ./code/code2.sh data/data1 data/data2
dvc run ./code/code3.sh data/data2 data/data3
dvc run ./code/code4.sh data/data3 data/data4

dvc config Global.Target data/data4
git commit -am 'Set Target'

# Create new branch to work on small data set
git checkout -b input_10K
dvc remove -c -l data/data
dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/10K/Posts.xml.tgz data/data
dvc repro

echo -e "#!/bin/bash\n\n\n\ncp \$1 \$2" > code/code1.sh
git commit -am"change code1.sh"
dvc repro

# Merge input_10K into input_50K
git checkout input_50K
git merge input_10K
# Notice we're running 'dvc merge' on input_50K branch. This
# way dvc knows to favor data files from current branch.
dvc merge

# Verify that data and respective state file are restored properly
if [ "$(dvc_md5 data/data)" != "$ORIG_DATA" ]; then
	dvc_fail
fi

grep $(echo "$ORIG_DATA" | cut -d " " -f1) .dvc/state/data/data.state || dvc_fail

dvc_pass
