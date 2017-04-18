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
Info. Directories data/, cache/ and state/ were created
Info. File .gitignore was created
Info. Directory cache was added to .gitignore file
Info. [Git] A new commit a5ed2b9 was made in the current branch. Added files:
Info. [Git] A  .gitignore
Info. [Git] A  dvc.conf
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
$ dvc data-import https://s3-us-west-2.amazonaws.com/dataversioncontrol/functests/stackoverflow_raw_small/Badges.xml data/
$ dvc data-import https://s3-us-west-2.amazonaws.com/dvc-doc/small/Badges.xml data/
Info. [Git] A new commit 9f27943 was made in the current branch. Added files:
Info. [Git] A  data/Badges.xml
Info. [Git] A  state/Badges.xml.state
```

The data file was downloaded and now it is available in the data directory.
However, it is only a part of the work that dvc has done.

If you look carefully at the file you could see that this is actually a symlink to
    a cache directory file Badges.xml_a5ed2b9:
```
$ ls -l data/Badges.xml
lrwxr-xr-x  1 dmitry  staff    27B Apr 17 16:19 data/Badges.xml@ -> ../cache/Badges.xml_a5ed2b9
```

Instead of importing the file to `data/Badges.xml` the system imports the file in the cache directory
    `cache/Badges.xml_a5ed2b9` and creates a symlink `data/Badges.xml`.
The file in the cache directory `cache/Badges.xml_a5ed2b9` has the same name as 
    the file in the data directory but extended by a suffix `_a5ed2b9` which
    correspondes to a git snapshot checksum on the time when the file was created.



The system transparently moves the actual file to the cache directory and creates a symlink




1. `data/` directory was created to keep all your data files that not suppose to go to Git repository.
This is the directory users suppose to use for all the data files.
However, dvc transparently moves all actual files to the `cache` directory and keeps only 
    symlink in `data/`.
2. `cache/` directory is a directory the file content.


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
