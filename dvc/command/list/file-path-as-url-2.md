```console
# Continuation of file-path-as-url-1.md above.
$ dvc pull featurize.dvc
$ dvc list featurize.dvc  # With target DVC-file
INFO: Listing LOCAL DVC project files, directories, and data at
      /home/uname/example-get-started/
INFO: Limiting list to data outputs from featurize.dvc stage.

367B 2019-09-20 featurize.dvc
2.7M            └ out: data/features/test.pkl
 11M            └ out: data/features/train.pkl
```
