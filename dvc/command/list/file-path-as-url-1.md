```console
$ git clone git@github.com:iterative/example-get-started.git
$ cd example-get-started
$ dvc list .
INFO: Listing LOCAL DVC project files, directories, and data at
      /home/uname/example-get-started/

 17B 2019-09-20 .gitignore
6.0K 2019-09-20 README.md
  9B 2019-09-20 auc.metric
128B 2019-09-20 data/
415B 2019-09-20 evaluate.dvc
367B 2019-09-20 featurize.dvc
337B 2019-09-20 prepare.dvc
224B 2019-09-20 src/
339B 2019-09-20 train.dvc
5.8M            └ out: (model.pkl)

WARNING: There are missing data files in the given path (shown in parentheses).
         Use dvc status to review them or dvc pull to download them.
         See http://man.dvc.org/status and http://man.dvc.org/pull.
```
