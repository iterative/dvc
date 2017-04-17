[![Code Climate](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/gpa.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol)
[![Test Coverage](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/coverage.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol/coverage)

# Data version control
Git for data science projects. It streamlines your git code, your data (S3 and GCP) and the dependencies to a single reproducible environment.

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
