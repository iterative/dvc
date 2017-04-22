[![Code Climate](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/gpa.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol)
[![Test Coverage](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/coverage.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol/coverage)

# Data version control
Git for data science projects. It streamlines your git code, your data (S3 and GCP) and dependencies to a single reproducible environment.


# Tutorial

## Install

Please download dvc from http://dataversioncontrol.com for your platform or
    install through source code from Github https://github.com/dmpetrov/dataversioncontrol.

## Initialize dvc environment

DVC is an extension of the git command.
It works on top of git command in existing git repositories.
Let's create a new git repository for the tutorial perpose.

```
$ cd ~/src
$ mkdir tag_classifier
$ cd tag_classifier
$ git init
Initialized empty Git repository in /Users/dmitry/src/tag_classifier/.git/
```

Initialize dvc environment:

```
$ dvc init
Directories data/, .cache/ and .state/ were created
File .gitignore was created
Directory .cache was added to .gitignore file
[Git] A new commit e7a4b0d was made in the current branch. Added files:
[Git]   A  .gitignore
[Git]   A  dvc.conf
```

The output shows the actions that dvc has made.
Special directories `data/`, `cache/` and `state/` were created to keep track of your external data which are not stored in git but have to be under users control.
These three directories are mondatory for any dvc project.

Names of the directory might be changed by the `dvc init` command line parameters `--data-dir`, `--cache-dir` and `--state-dir` respectively.
All these special directories names are going to be saved in `dvc.conf` config file as a result of 
    the init command and should not be changed over dvc project life time.

As you might see from the output `.gitignore` file was created (or modified if you already had one).
This is result of including cache directory to this file and also `.dvc.conf.lock` file.
Meaning of these changes will be explain later in the tutorial.

DVC commits all the chenges to the git repository at the end of successful run.
The commit step might be skipped by command line parameter `--skip-git-actions`.
This parameter might be applied to any dvc command.

## Importing data files

Data file has to be imported in dvc `data/` directory before using it.

```
$ dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/small/Posts.xml.tgz data/
[Git] A new commit b69ab31 was made in the current branch. Added files:
[Git]   A  .state/Posts.xml.tgz.state
[Git]   A  data/Posts.xml.tgz
```

The data file was downloaded and now it is available in the data directory.
However, it is only a part of the work that dvc has done.

### Cache files

If you look carefully at the file you could see that this is actually a symlink to
    a cache directory file Badges.xml_a5ed2b9:
```
$ ls -l data/Posts.xml.tgz
lrwxr-xr-x  1 dmitry  staff    31B Apr 21 17:56 data/Posts.xml.tgz@ -> ../.cache/Posts.xml.tgz_9185f92
```

Instead of importing the file to `data/Badges.xml` the system imports the file in the cache directory
    and creates a symlink `data/Badges.xml`.
The file in the cache directory `cache/Badges.xml_a5ed2b9` has the same name as 
    the file in the data directory but extended by a suffix `_a5ed2b9` which
    correspondes to a git snapshot checksum on the time when the file was created.

### State files

In addition to the cache file dvc creates a state file in the `state/` directory for each data file:
```
$ ls -l .state/Posts.xml.tgz.state
-rw-r--r--  1 dmitry  staff   437B Apr 21 17:56 .state/Posts.xml.tgz.state
```

The state file contains information for file reproducibility.


## Structure data


```python
import sys
import xml.etree.ElementTree

def print_usage(msg):
    if msg:
        sys.stderr.write('{}\n'.format(msg))
    sys.stderr.write('Usage:\n')
    sys.stderr.write('\tpython posts_to_tsv.py INPUT OUTPUT\n')


def process_posts(fd_in, fd_out):
    num = 1
    for line in fd_in:
        try:
            attr = xml.etree.ElementTree.fromstring(line).attrib
            items = (
                attr.get('Id', ''),
                attr.get('PostTypeId', ''),
                attr.get('CreationDate', ''),
                attr.get('Score', ''),
                attr.get('ViewCount', ''),
                attr.get('OwnerUserId', ''),
                attr.get('LastActivityDate', ''),
                attr.get('AnswerCount', ''),
                attr.get('CommentCount', ''),
                attr.get('FavoriteCount', ''),
                attr.get('Title', ''),
                attr.get('Body', '')
            )
            fmt = ('{}\t' * len(items))[:-1] + '\n'

            fd_out.write(fmt.format(*items))

            num += 1
        except Exception as ex:
            sys.stderr.write('Error in line {}: {}'.format(num, ex))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print_usage('Paramet error.')
        sys.exit(1)

    input = sys.argv[1]
    if not os.path.exist(input):
        print_usage('Input file {} does not exist'.format(input))
        sys.exit(1)

    with open(input) as fd_in:
        with open(sys.argv[2], 'w') as fd_out:
            process_posts(fd_in, fd_out)
```


```
$ cd data
$ dvc run tar zxf Posts.xml.tgz
[Git] A new commit fbff960 was made in the current branch. Added files:
[Git]   A  .state/Posts.xml.state
[Git]   A  data/Posts.xml
$ cd ..
```


```
$ wget -P code/ https://s3-us-west-2.amazonaws.com/dvc-share/so/small/posts_to_tsv.py
```

```
$ git add code/posts_to_tsv.py
$ git commit -m 'Post to tsv script'
[master 4a581d9] Post to tsv script
 1 file changed, 51 insertions(+)
  create mode 100644 code/posts_to_tsv.py
```


```
$ dvc run python code/posts_to_tsv.py data/Posts.xml data/Posts.tsv
[Git] A new commit dc54e5a was made in the current branch. Added files:
[Git]   A  .state/Posts.tsv.state
[Git]   A  data/Posts.tsv
```

# Copyright

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request for this project, you agree to license your contribution under the Apache license version 2.0 to this project.

# Setup / Configuration
`dvc` supports both aws and google cloud; you will need accounts either from [aws](https://aws.amazon.com/) or [gcloud](https://cloud.google.com/).

Edit `dvc.conf`, and fill in `StoragePath` under `[Data]` with your preferred bucket and path.  For example,
```
[Data]
StoragePath = gc://dvc-demo/dvcdata
```

## Amazon AWS Setup
NB: To see your available buckets, run `aws s3 ls`

Configuration:
* edit `dvc.conf`, and set `Cloud = amazon`
* under `[AWS]`, fill in `AccessKeyId` and `SecretAccessKey`

Test `dvc` is correctly configured by running `dvc test-aws` (TODO)

## Google Cloud Setup
NB: To see your available buckets, run `gsutil ls`.

Configuration:
* edit `dvc.conf`, and set `Cloud = google`
* under `[GC]`, set `ProjectName` to your preferred (or just your default) project name.

Test `dvc` is correctly configured by running `dvc test-gcloud`.


# Usage
```
mkdir t
cd !$
git init .
dvc init

Info. Directories data/, cache/ and state/ were created
Info. File .gitignore was created
Info. Directory cache was added to .gitignore file
Info. [Git] A new commit 34687b2 was made in the current branch. Added files:
Info. [Git]	A  .gitignore
Info. [Git]	A  dvc.conf



```

# Development
Create a virtualenv, for example, at `~/.environments/dvc` by `mkdir -p ~/.environments/dvc; virtualenv ~/.environments/dvc` or use `virtualenvwrapper`.

```
# if you use virtualenvwrapper
workon dvc

# otherwise:
source ~/.environments/dvc/bin/activate

pip install -r requirements.txt

# happy coding!
```

# Building
## OSX
```
# creates dist/dvc
./build_osx.sh
```
