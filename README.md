[![Code Climate](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/gpa.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol)
[![Test Coverage](https://codeclimate.com/github/dmpetrov/dataversioncontrol/badges/coverage.svg)](https://codeclimate.com/github/dmpetrov/dataversioncontrol/coverage)

# Data version control
Data version control for data science projects. Streamline your git code, your data (S3 and GCP) and the dependencies to a single reproducible environment.

# Copyright

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request for this project, you agree to license your contribution under the Apache license version 2.0 to this project.


# Development
Create a virtualenv, for example, at `~/.environments/dvc` by `mkdir -p ~/.environments/dvc; virtualenv ~/.environments/dvc` or use `virtualenvwrapper`.

```
workon dvc

pip install -r requirements.txt
```

# Building
## OSX
```
# creates dist/dvc
./build_osx.sh
```
