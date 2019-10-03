```console
$ dvc list --recursive https://github.com/iterative/dataset-registry tutorial  # Recursive inside target dir
INFO: Limiting list to files and directories in tutorial/
INFO: Expanding list recursively.

 29B 2019-08-29 tutorial/nlp/.gitignore
178B 2019-08-29 tutorial/nlp/Posts.xml.zip.dvc
 10M            └ out: tutorial/nlp/Posts.xml.zip
177B 2019-08-29 tutorial/nlp/pipeline.zip.dvc
4.6K            └ out: tutorial/nlp/pipeline.zip
 26B 2019-08-27 tutorial/ver/.gitignore
173B 2019-08-27 tutorial/ver/data.zip.dvc
 39M            └ out: tutorial/ver/data.zip
179B 2019-08-27 tutorial/ver/new-labels.zip.dvc
 22M            └ out: tutorial/ver/new-labels.zip
```
