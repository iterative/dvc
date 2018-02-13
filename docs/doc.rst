========
Why DVC?
========

Why? Really.

=============================
What is DVC?
=============================

Data Version Control or DVC is a new type of **experiment management software** that has built on top of source code version control system (currently - Git).
This gives the ability to **use the best engineering practices in data science projects**.

The underlying source control system **eliminates the need of using external services**.
Experiment sharing and data scientists collaboration can be done by a regular Git tools (such as commit messages, merges, pull requests, code comments) the same way as it works for software engineers.

DVC implements an **Git experimentation methodology** where each experiment with it's code as well as data is presented as a separate Git branch or commit.

DVC uses a few concepts:

- **Experiment** is equal to Git branch. Each experiment (extract new features, change model hyperparameters, clean noise in data, add a new data source) should be performaed in a separate branch and then merged into the master branch if the experiment was successfull. DVC manages to NOT recompute the results after such merges.

- **Experiment state** or state is equal to Git snapshot (all commited files). Git checksum, a branch name or a tag can be used as a reference to an experiment state.

- **Reproducability** - an action to reproduce an experiment state. The action generates output files based on a set of input files and source code. This action usually changes experiment state.

- **Pipeline** - directed acyclic graph (DAG) of commands to reproduce an experiment state. The commands are connected by input and output files. Pipeline is defined by special **dvc-files** (like Makefiles).

- **Workflow** - set of experiments and relationships between them. Workflow corresponds to the entire Git repository.

- **Data files** - cached files (for large files). For data files the file content is stored outside of Git repository in a local hard drive but data file meta information is stored in Git for DVC needs (to maintain pipilines and reproducibility).

- **Data cache** is a directory with all data files content in a local hard drive or a cloud storage but not in Git repository.

- **Cloud storage** support is a compliment to the core DVC features. This is how a data scientist transfers large data files or shares a trained on GPU model to whom who does not have GPU.


=============
Core features
=============

1. DVC works **on top of Git repositories** and has a similar command line interface and Git workflow.

2. It makes data science projects **reproductable** by creating lightweight pipelines (DAG).

3. **Large data file versioning** by creating pointers in your Git repository into a data cache in a local hard drive.

4. **Programming language agnostic**: Python, R, Julia, shell scripts, etc. ML library agnostic: Keras, Tensorflow, PyTorch, scipy, etc.

5. **Open sourced** and **Self-served**. DVC is free and does not require any additional services.

6. DVC supports cloud storges (AWS S3 and GCP storage) for **data and pretrained models sharing**.


====================
Related technologies
====================

Due to the the novelty of this approach DVC can be understood better in comparision to the existing technologies and ideas.

DVC combines numbers of existing technologies and ideas into a single product with a goal of bringing the best engineering practices into data sciance process. This comparision can give you good understanding of DVC if you are familliar with some of these technologies.

1. **Git** is an underlying technology for DVC. Difference:

   - DVC extends Git by concept of *data files* - large files that should NOT be stored in Git repository but still have to be tracked and versioned. See more details in Git-annexand Git-LFS items below.

2. **Workflow management tools** (pipelines and DAGs): Apache Airflow, Luigi and etc. Differences:
   
   - Comparing to these data engineering technologies, DVC is focused on data science and modeling. As a result, DVC pipelines are lightweight, easy to create and modify. However, DVC lacks of pipeline execution features like execution monitoring and execution error handling and recovering.

   - DVC is a purly command line tool that does not have user interface and does not run any servers. Nevertheless, DVC can generate images with pipeline and experiment workflow visualization.

3. Today **experiment management** software are mostly designed for enterprise usage. Open sourced experimentation tool example: http://studio.ml/. Differences:

   - DVC uses Git as underlying platform for experiment tracking instead of a web application.

   - There is no need to run any services. No user interface as a result. Nevertheless, we expect some UI sevices will be created on top of DVC.

   - DVC has transparent design: dvc-files, meta files, state file, cache dirs have a simple (usually text) format and can be easily reused by external tools.

4. **Git workflows** and Git usage methodologies such as Gitflow. Differences:

   - DVC supports a new experimentation methodology (and Git workflow) when a separate branch should be created for each experiment with unpredictable results with subsequent merge of this branch if the result was successful. An experiment can have multiple sub experiments.

   - One of the major DVC innovation is the ability to navigate through the experiments without recomputing them.


5. DVC uses the **Makefile** (and it's analogues) idea of defining DAG by a special files. Differences:

   - The DAG definition:

     - The DAG is defined by dvc-files with filenames *Dvcfile* or *<filename>.dvc*, not *Makefile*.

     - One dvc-file defines one node in DAG. All dvc-files in a repository make up a single pipeline (think a single Makefile). All dvc-files (and corresponded pipeline commands) are imlicitly combined through theirs inputs and outputs. This file separation is done to simplify conflict resolving during Git merges.

     - DVC provide a simple command *dvc run CMD* to generate dvc file automaticaly based on the provided command, dependencies and outputs.

   - File tracking:

     - DVC tracks files based on checksum (md5) instead of file timesteps. This helps to avoid of running heavy process like model re-training when you checkout a previous, already trained version of a modeling code (Makefile will retrain a model).

     - Despite the existence of file checksum tracking, DVC still uses the files timestemps and inodes and keep them in a local state file for optimization. Otherwise, each reproduction command will require recomputing all dependency files checksum - unappropriate approach for large files (10Gb+).


6. **Git-annex**. DVC uses the idea of storing the content of large files (that you don't what to see in your Git repository) in a local key-value store and use file symlinks insted of the actual files. Difference:

   - DVC uses hardlinks instead of symlinks to make user experience better.
   
   - DVC optimizes checksum calculation (see Makefile differences from the above).

   - DVC stores data file mata information in Git repository *.dvc/*, not in the Git tree *.git/annex/*. As a result, all meta information can be shared through any Git server like Github (Git-annex loses all meta information when shared by Git server).

7. **Git-LFS** (Large File Storage) interface is much user friendly comparing to Git-annex. It requires a very limited number of additional command. DVC uses this methodology - native Git like interface with a couple additional file related commands. Differences:

   - DVC is fully compatable with Git. It does not required special Git servers like Git-LFS does.

   - DVC does not add any hooks to Git by default. To checkout data files *dvc checkout* command has to be run after each *git checkout* and *git clone* command.

   - *dvc checkout* command does not actually copy data files from cache to the working tree because of the file copying command is a heavy operation for large files (30Gb+). DVC creates hardlinks instead and changes data file permissions to read only.


==============
How it works?
==============

1. DVC is a command line tool that works on top of Git::

	$ cd my_git_repo
	$ dvc init

2. It helps define pipelines of your commands and keeps all the commands and the dependencies (DAG) in Git repository::

	$ dvc run -d input.csv -o results.csv python cnn_train.py --seed 20180227 --epoch 20 input.csv result.csv
	$ git add result.csv
	$ git commit -m 'Train CNN. 20 epochs.'

3. DVC is programming language agnostic. R command example::

	$ dvc run -d result.csv -o plots.jpg Rscript plot.R result.csv plots.jpg
	$ git add plots.jpg
	$ git commit -m 'CNN plots'

4. Reproduce a pipeline with respect to dependencies::

	# The input dataset was changed
	$ dvc repro plots.jpg
	Reproducing 'output.p':
	    python cnn_train.py --seed 20180227 --epoch 20 input.csv output.p
	Reproducing 'plots.jpg':
	    Rscript plot.R result.csv plots.jpg

5. DVC introduces data files concept for Git repository. DVC keeps data files outside of Git repository but meta data in Git::

	$ git checkout a03_normbatch_vgg16 # checkout code and DVC meta data
	$ dvc checkout # checkout data files from the local cache (not Git)
	$ ls -l data/ # These LARGE files were copied from DVC cache, not from Git
	total 1017488
	-r--------  2 501  staff   273M Jan 27 03:48 Posts-test.tsv
	-r--------  2 501  staff    12G Jan 27 03:48 Posts-train.tsv


6. Reproductable DVC repository with DVC meta information (DAG) can be easily shared by any Git server::

	$ git clone https://github.com/dataversioncontrol/myrepo.git
	$ cd myrepo
	# Reproduce data files
	$ dvc repro
	Reproducing 'output.p':
	    python cnn_train.py --seed 20180227 --epoch 20 input.csv output.p
	Reproducing 'plots.jpg':
	    Rscript plot.R result.csv plots.jpg

7. DVC local cache can be transfered to your collegues and parthners through AWS S3 or GCP Storage::

	$ git push
	$ dvc push # push the data cache to your cloud bucket
	
	# On a collegue machine:
	$ git clone https://github.com/dataversioncontrol/myrepo.git
	$ cd myrepo
	$ git pull # get the data cache from cloud
	$ dvc checkout # checkout data files
	$ ls -l data/ # You just got gigabites of data though Git and DVC:
	total 1017488
	-r--------  2 501  staff   273M Jan 27 03:48 Posts-test.tsv

8. DVC works on Mac, Linux and Windows systems. Windows example::

	$ dir
	?????
	????


==============================
Getting Started with DVC (old)
==============================

To show DVC in action let's play with an actual machine learning (ML) scenario.
This is going to be a natural language processing (NLP) problem of predicting tags for given 
    stackoverflow question.
For instance, for the tag "Java" one classifier will be created which can predict a post that is about the Java language.

First, let's download modeling code and set up Git repository::

	$ mkdir myrepo
	$ cd myrepo
	$ mkdir code
	$ wget -nv -P code/ https://s3-us-west-2.amazonaws.com/dvc-share/so/code/featurization.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/evaluate.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/train_model.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/split_train_test.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/xml_to_tsv.py \
        https://s3-us-west-2.amazonaws.com/dvc-share/so/code/requirements.txt
	$ pip install -U -r code/requirements.txt
	$ git init
	$ git add code/
	$ git commit -m 'Download code'

The full pipeline could be built by running the code below::

	$ # Initialize DVC repository (in your Git repository)
	$ dvc init
	
	$ # Download a file and put to data/ directory.
	$ dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/
	
	$ # Extract XML from the archive.
	$ dvc run tar zxf data/Posts.xml.tgz -C data/
	
	$ # Prepare data.
	$ dvc run python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
	
	$ # Split training and testing dataset. Two output files.
	$ # 0.33 is the test dataset splitting ratio. 20170426 is a seed for randomization.
	$ dvc run python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
	
	$ # Extract features from text data. Two TSV inputs and two pickle matrixes outputs.
	$ dvc run python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p
	
	$ # Train ML model out of the training dataset. 20170426 is another seed value.
	$ dvc run python code/train_model.py data/matrix-train.p 20170426 data/model.p
	
	# Evaluate the model by the testing dataset.
	$ dvc run python code/evaluate.py data/model.p data/matrix-test.p data/evaluation.txt
	
	$ # The result.
	$ cat data/evaluation.txt
	AUC: 0.596182


DVC an open source tool for data science projects. DVC makes your data science projects reproducible by automatically building data dependency graph (DAG). Your code and the dependencies could be easily shared by Git, and data - through cloud storage (AWS S3, GCP) in a single DVC environment.

Your code can be easily reproduced after modification::

	$ # Improve feature extraction step.
	$ vi code/featurization.py
	
	$ # Commit all the changes.
	$ git commit -am "Add bigram features"
	[master 50b5a2a] Add bigram features
	1 file changed, 5 insertion(+), 2 deletion(-)
	
	$ # Reproduce all required steps to get our target metrics file.
	$ dvc repro data/evaluation.txt
	Reproducing run command for data item data/matrix-train.p. Args: python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p
	Reproducing run command for data item data/model.p. Args: python code/train_model.py data/matrix-train.p 20170426 data/model.p
	Reproducing run command for data item data/evaluation.txt. Args: python code/evaluate.py data/model.p data/matrix-test.p data/evaluation.txt
	Data item "data/evaluation.txt" was reproduced.
	
	$ # Take a look at the target metric improvement.
	$ cat data/evaluation.txt
	AUC: 0.627196

It is quite easy to integrate DVC in your existing ML pipeline/processes without any significant effort to re-implement your ML code/application.

The one thing to wrap your head around is that DVC automatically derives the dependencies between the steps and builds the dependency graph (DAG) transparently to the user. This graph is used for reproducing parts of your pipeline which were affected by recent changes.

Not only can DVC streamline your work into a single, reproducible environment, it also makes it easy to share this environment by Git including the dependencies (DAG) — an exciting collaboration feature which gives the ability to reproduce the research results in different computers. Moreover, you can share your data files through cloud storage services like AWS S3 or Google Cloud Project (GCP)  Storage since DVC does not push data files to Git repositories.


============
Installation
============

Operation system dependent packages is the recommended way of installing DVC.
Some other methods of installation are provided.

OS packages
===========

DVC installation packages available for Mac OS, Linux and Windows platforms.
You can download the packages at https://github.com/dataversioncontrol/dvc/releases/

Python pip
==========

Another option to deploy DVC to your machine is to use its standard Python pip package::

	$ pip install dvc

**Note:** if you use *Anaconda*, you can use the above-mentioned command there as well.
It will work in *Anaconda’s* command prompt tool.
As of the moment, DVC does not provide a special installation package for a native *Anaconda* package manager (that is, *conda*).

Homebrew Cask
=============

Mac OS users can install DVC by **brew** command::

	$ brew cask install dataversioncontrol/homebrew-dvc/dvc

Development Version
===================

If you like to pull the latest version of DVC from the master branch in its repo at github, you execute the following command in your command prompt::

	$ pip install git+git://github.com/dataversioncontrol/dvc

This command will automatically upgrade your DVC version in case it is behind the latest version in the master branch of the github repo.


=============
Configuration
=============

Once you install DVC, you should be able to start using it (in its local setup) immediately. 

However, you can optionally proceed to further configure DVC (especially if you intend to use it in a Cloud-based scenario).

DVC Files and Directories
=========================

Once installed, dvc will populate its installation folder (hereinafter referred to as .dvc) with essential shared and internal files and folders will be stored

* **.dvc/config** - This is a configuration file.
  The config file can be edited directly or indirectly using command **dvc config NAME VALUE**.
* **.dvc/cache** - the cache directory will contain your data files (the data directories of DVC repositories will only contain symlinks to the data files in the global cache).
  **Note:** DVC includes the cache directory to **.gitignore** file during the initilization. And no data files (with actual content) will be pushed to Git repository,
  only data file symlinks and commands to reproduce them.
* **.dvc/state** - this file is ceated for optimization. The file contains data files checksum and timestemps.


Working with Cloud Data Storages
======================================================

Using DVC with Cloud-based Data Storages is an optional feature.
By default, DVC is configured to use local data storage only (.dvc/cache directory),
  and it enables basic DVC usage scenarios out of the box.

DVC can use cloud storages as a common file storage.
With cloud storage you might use models and data file which were created by your team members
  without spending time and resources for re-building models and re-processing data files.

As of this version, DVC supports two types of cloud-based data storage providers:

* **AWS** - Amazon Web Services
* **GCP** - Google Cloud Provider

The subsections below explain how to configure DVC to use of the data cloud storages above.

Using AWS Cloud
---------------

For using AWS as a data cloud storage for your DVC repositories, you should update **.dvc/config** options respectively

* **Cloud = AWS** in *Global* section.
* **StoragePath = /mybucket/dvc/tag_classifier** in **AWS** section - path to a cloud storage bucket and directory in the bucket.
* **CredentialPath = ~/aws/credentials** in **AWS** section - path to AWS credentials in your local machine (AWS cli command line tools creates this directory).
  In Mac, default value is *~/.aws/credentials*, and it is *%USERPATH%/.aws/credentials* in Windows


**Important:** do not forget to commit the config file change to Git: **git commit -am "Change cloud to AWS"**

Instead of manual file modification we recommend to run corresponded commands::

	$ dvc config Global.Cloud AWS # This step is not needed for new DVC repositories
	$ dvc config AWS.StoragePath /mybucket/dvc/tag_classifier 
	$ dvc config AWS.CredentialPath ~/.aws/credentials # Not needed if aws cli is instelled to default path
	$ dvc config AWS.CredentialSection default # Not needed if you have only one AWS account
	$ git commit -am "Change cloud to AWS"


Using Google Cloud
------------------

For using GCP (Google Cloud Provider) as a data cloud storage for your DVC repositories, you should update **.dvc/config** options respectively

*  **Cloud = GCP** in *Global* section.
* **StoragePath = /mybucket/dvc/tag_classifier** in GCP section - this option has the same meaning as AWS one above. Run **dvc config GCP.StoragePath /my/path/to/a/bucket**
* **ProjectName = MyCloud** - a GCP specific project name.

**Important:** do not forget to commit the config file change to Git: **git commit -am "Change cloud to GCP"**

Instead of manual file modification we recommend to run corresponded commands::

	$ dvc config Global.Cloud GCP
	$ dvc config GCP.StoragePath /mybucket/dvc/tag_classifier 
	$ dvc config GCP.ProjectName MyCloud
	$ git commit -am "Change cloud to AWS"


==================
Using DVC Commands
==================

DVC is a command-line tool.
The typical method of use of DVC is as follows

* In an existing Git repository initialize a DVC repository with **dvc init** command.
* Copy source files for modeling into the repository (without checking out to Git) and convert the files in DVC data files with **dvc add** command.
* Process source data files by your data processing and modeling code through **dvc run** command. In this command generated DVC files to describe these processing steps.
* Use **--outs** option to specify **dvc run** command outputs which should be to be converted to DVC data files after the code is completed.
* You clone a git repo with the code of your ML application pipeline. However, it does not copy DVC cache. Use cloud storage settings and **dvc push** command to share the cache (data).
* You use **dvc repro** command to quickly reproduce your pipeline on a new iteration, once either the data item files or the source code of your ML application are modified.

========================
DVC Commands Cheat Sheet
========================

Below is the quick summary of the most important commands of DVC

* **dvc -h** - Show how to use DVC and show the list of commands.
* **dvc CMD -h** - Display help to use a specific DVC command (CMD)
* **dvc init** - Initialize a new DVC repository.  
* **dvc add** - Add data file or data directory. The command converts regular files to DVC data files.
* **dvc checkout** - Checkout data files and dirs into working tree. The command should be executed after **git checkout** or cloning a repository.
* **dvc run** - Generate a DVC file from a given command and execute the command. The command dependencies and outputs should be specified.
* **dvc pull** - Pull data files from the cloud. Cloud settings for your DVC environment should be already configured prior to using this command.
* **dvc push** - Push data files to the cloud. Cloud settings should be already configured.
* **dvc status** - Show status of a data file in the DVC repository.
* **dvc repro** - Reproduce a stage of pipeline. Default stage file is **Dvcfile**.
* **dvc remove** - Remove data file (files or/and folders).
* **dvc gc** - Collect garbage by cleaning DVC cache.
* **dvc config** - Get or set configuration settings (as specified in dvc.conf).
* **dvc show** - Show graphs.
* **dvc fsck** - Data file consistency check.

=====================
DVC Command Reference
=====================

init
====

This command initializes a DVC environment in a current Git repository.

.. code-block:: shell
   :linenos:

	usage: dvc init [-h] [-q] [-v]
	optional arguments:
	  -h, --help     show this help message and exit
	  -q, --quiet    Be quiet.
	  -v, --verbose  Be verbose.

Example. Creating a new DVC repository::

	$ mkdir tag_classifier
	$ cd tag_classifier
	
	$ git init
	Initialized empty Git repository in /Users/dmitry/src/tag_classifier/.git/
	
	$ dvc init
	$ git status
	On branch master
	
	Initial commit
	
	Changes to be committed:
	
	  (use "git rm --cached <file>..." to unstage)
	
	        new file:   .dvc/.gitignore
	        new file:   .dvc/config
	
	$ git commit -m 'Init DVC'
	[master (root-commit) 2db4618] Init DVC
	 2 files changed, 41 insertions(+)
	 create mode 100644 .dvc/.gitignore
	 create mode 100644 .dvc/config


add
====

Converts files and directories to DVC data files.

The command doe the convertation from a *regular file* to DVC data file in a few steps:

1. Calculate the file checksum.
2. Create a cache file in the cache dir *.dvc/cache* with the content of this file.
3. Create a corresponded DVC file.
4. Replace the file by a hardlink to the cache file.

Also, to reduce time on recomputing the file checksum in future DVC stores the file last modification time, inode and the checksum into a global state file *.dvc/state*.
Next time, then the file chacksum will be needed DVC will try to get it from the file if the file was not modified.

Note, this command does NOT copy any file content and run quickly even for a large files.
Step (2) from the above is also made by hardlinks movement, not file content.
The only haavy step is (1) which requires checksum calculation.

For directories the command does the same steps for each file recursively.
To keep information about the directory structure a corresponded directory will be created in the cache *.dvc/cache*.

.. code-block:: shell
   :linenos:

	usage: dvc add [-h] [-q] [-v] targets [targets ...]

	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.

Examples:

Convert files into data files::

	$ mkdir raw
	$ cp ~/Downloads/dataset/* raw
	$ ls raw
	Badges.xml          PostLinks.xml           Votes.xml
	$ dvc add raw/Badges.tsv raw/PostLinks.tsv raw/Votes.tsv
	$ ls raw
	Badges.xml          PostLinks.xml           Votes.xml
	Badges.xml.dvc      PostLinks.xml.dvc       Votes.xml.dvc

Note, DVC files were created.


checkout
========

Checkout data files from cache.
This command has to be called after *git checkout* since Git does not handle DVC data files.

The command restores data files from cache to working tree and removes data files that are not belog to the current working tree anymore.

Note, this command does NOT copy any files - DVC uses hardlinks to perform the data file restoring.
This is crucial for large files where checking out (copiyng) 50Gb file might take a few minutes.
For DVC it will take less than a second to restore 50Gb data file.


.. code-block:: shell
	:linenos:

	usage: dvc checkout [-h] [-q] [-v]

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.

Examples.

Checking out a branch example::

	$ git checkout input_100K
	$ dvc checkout
	$ Remove 'data/model.p'
	$ Remove 'data/matrix-train.p'
	$ 'data/Posts-train.tsv': cache file not found

DVC does not report in the output which data files were restored.
However, it reports removed files and files which DVC was unable to restore because of missing cache.
To restore file with missing cache reproduction command should be called or cache can be pulled from a cloud.

It might be convinient to assign Git hook to *git checkout* comman::

	$ echo 'dvc checkout' > .git/hooks/post-checkout
	$ chmod +x .git/hooks/post-checkout
	$ git checkout input_100K  # dvc checkout is not needed anymore
	$ Remove 'data/model.p'
	$ Remove 'data/matrix-train.p'
	$ 'data/Posts-train.tsv': cache file not found

run
===

Generate a stage file from a given command and execute the command.
The command dependencies and outputs should be specified.

By default stage file name is **<file>.dvc** where **<file>** is file name of a first output.

For example, launch Python with a given python script and arguments. Or R script by Rscript command.

.. code-block:: shell
   :linenos:
   
	usage: dvc run [-h] [-q] [-v] [-d DEPS] [-o OUTS] [-O OUTS_NO_CACHE] [-f FILE]
	               [-c CWD] [--no-exec]
	               ...
	
	positional arguments:
	  command               Command or command file to execute
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.
	  -d DEPS, --deps DEPS  Declare dependencies for reproducible cmd.
	  -o OUTS, --outs OUTS  Declare output data file or data directory.
	  -O OUTS_NO_CACHE, --outs-no-cache OUTS_NO_CACHE
	                        Declare output regular file or directory (sync to Git,
	                        not DVC cache).
	  -f FILE, --file FILE  Specify name of the state file
	  -c CWD, --cwd CWD     Directory to run your command and place state file in
	  --no-exec             Only create stage file without actually running it

Examples:

Execute a Python script as a DVC ML pipeline step. Stage file was not specified. So, **model.p.dvc** stage file will be created::

	$ # Train ML model out of the training dataset. 20180226 is a seed value.
	$ dvc run -d matrix-train.p -d train_model.py -o model.p python train_model.py matrix-train.p 20180226 model.p


Execute an R script as a DVC ML pipeline step::

	$ dvc run -d parsingxml.R -d Posts.xml -o Posts.csv Rscript parsingxml.R Posts.xml Posts.csv


Extract an XML file from an archive to data subfolder::

	$ mkdir data
	$ dvc run -d Posts.xml.tgz -o data/Posts.xml tar zxf Posts.xml.tgz -C data/


push
====

This command pushes all data files caches related to the current Git branch to the cloud storage.
Cloud storage settings need to be configured.
See cloud storage configuration.

.. code-block:: shell
   :linenos:

	usage: dvc push [-h] [-q] [-v] [-j JOBS]

	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.
	  -j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Push all data files caches from the current Git branch to cloud::

	$ dvc push
	(1/8): [########################################] 100% 72271bebdf053178a5cce48b4
	(2/8): [########################################] 100% d7208b910d1a40fedc2da5a44
	(3/8): [########################################] 100% 7f6ed2919af9c9e94c32ea13d
	(4/8): [########################################] 100% 5988519f8465218abb23ce0e0
	(5/8): [########################################] 100% 11de13709a78379d253a3d0f5
	(6/8): [########################################] 100% 3f9c7c3ae51db2eed7ba99e6e
	(7/8): [########################################] 100% cfdaa4bba57fa07d81ff96685
	(8/8): [#######################                 ] 57% 1de6178a9dd844e249ba05414
	

pull
====

This command pulls all data files caches from the cloud storage.
Cloud storage settings need to be configured.

.. code-block:: shell
   :linenos:
   
	usage: dvc pull [-h] [-q] [-v] [-j JOBS]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.
	  -j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Pull all files from the current Git branch::

	$ dvc pull
	(1/8): [########################################] 100% 54a6f1787490ba13fb811a46b
	(2/8): [########################################] 100% 5806dc797c08fb6ddd5d97d46
	(3/8): [########################################] 100% 5988519f8465218abb23ce0e0
	(4/8): [########################################] 100% 7f6ed2919af9c9e94c32ea13d
	(5/8): [########################################] 100% 11de13709a78379d253a3d0f5
	(6/8): [########################################] 100% c6f5a256d628e144db4181de8
	(7/8): [########################################] 100% 3f9c7c3ae51db2eed7ba99e6e
	(8/8): [########################################] 100% cfdaa4bba57fa07d81ff96685

status
======

Show mismatches between local cache and cloud cache.

.. code-block:: shell
	:linenos:

	usage: dvc status [-h] [-q] [-v] [-j JOBS]
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.
	  -j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Show statuses::

	$ dvc status
	        new file:   /Users/dmitry/src/myrepo_1/.dvc/cache/62f8c2ba93cfe5a6501136078f0336f9

repro
=====

Reproduce DVC file and all stages the file depends on (recursively).
Default file name is **Dvcfile**.
However, usually DVC files have any name and **.dvc** suffix.

.. code-block:: shell
	:linenos:

	usage: dvc repro [-h] [-q] [-v] [-f] [-s] [targets [targets ...]]

	positional arguments:
		target                DVC file to reproduce.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-f, --force           Reproduce even if dependencies were not changed.
		-s, --single-item     Reproduce only single data item without recursive dependencies check.

Examples:

Reproduce default stage file::

	$ dvc repro
	Verifying data sources in 'data/Posts.xml.tgz.dvc'
	Reproducing 'Posts.xml.dvc':
	        tar zxf data/Posts.xml.tgz -C data/
	Reproducing 'Posts.tsv.dvc':
	        python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python
	Reproducing 'Posts-train.tsv.dvc':
	        python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv
	Reproducing 'matrix-train.p.dvc':
	        python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p
	Reproducing 'model.p.dvc':
	        python code/train_model.py data/matrix-train.p 20170426 data/model.p

Reproduce the part of the pipeline (from above) where *Posts.tsv.dvc* is target DVC file::

	$ dvc repro Posts.tsv.dvc
	Reproducing 'Posts.xml.dvc':
	        tar zxf data/Posts.xml.tgz -C data/
	Reproducing 'Posts.tsv.dvc':
	        python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python


remove
======

Remove data file or data directory.

.. code-block:: shell
	:linenos:

	usage: dvc remove [-h] [-q] [-v] targets [targets ...]

	positional arguments:
		targets               Target to remove - file or directory.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.

Examples:


Remove *matrix-train.p* data file::

	$ dvc remove matrix-train.p



gc
===

This command collects the garbage - removes unused cache files based on the current Git branch.
So, if a data file was created in a different branch then it is going to be removed by command.
If a data file has a few versions (and, correspondingly, caches) - all the chaches except the current one will be removed.

.. code-block:: shell
	:linenos:
	
	age: dvc gc [-h] [-q] [-v]

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.

Clean up example::

	$ du -sh .dvc/cache/
	7.4G    .dvc/cache/
	$ dvc gc
	'.dvc/cache/27e30965256ed4d3e71c2bf0c4caad2e' was removed
	'.dvc/cache/2e006be822767e8ba5d73ebad49ef082' was removed
	'.dvc/cache/2f412200dc53fb97dcac0353b609d199' was removed
	'.dvc/cache/541025db4da02fcab715ca2c2c8f4c19' was removed
	'.dvc/cache/62f8c2ba93cfe5a6501136078f0336f9' was removed
	'.dvc/cache/7c4521365288d69a03fa22ad3d399f32' was removed
	'.dvc/cache/9ff7365a8256766be8c363fac47fc0d4' was removed
	'.dvc/cache/a86ca87250ed8e54a9e2e8d6d34c252e' was removed
	'.dvc/cache/f64d65d4ccef9ff9d37ea4cf70b18700' was removed
	$ du -sh .dvc/cache/
	3.1G    .dvc/cache/


config
======

Get or set config options. This command reads and owerwrites DVC config file *.dvc/config*.


.. code-block:: shell
	:linenos:
	
	usage: dvc config [-h] [-q] [-v] [-u] name [value]

	positional arguments:
		name                  Option name
		value                 Option value

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-u, --unset           Unset option

Examples:

Specify an option name to get the option value from config file::
	$ dvc config config Global.Cloud
	AWS

Overwrite the value::

	$ dvc config Global.Cloud GCP
	$ git add .dvc/config
	$ git commit -m 'Change cloud to GCP'
	[input_100K a4c985f] Change cloud to GCP
	 1 file changed, 1 insertion(+), 1 deletion(-)

show
====

Generate pipeline image for your current project.

.. code-block:: shell
	:linenos:
	
	usage: dvc show [-h] [-q] [-v] {pipeline} ...

	positional arguments:
		{pipeline}     Use `dvc show CMD` --help for command-specific help
		pipeline              Show pipeline image

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.

Examples:

Show the pipeline image::

	$ dvc show pipeline

fsck
====

Data file consistency check.
By default the commands outputs statuses of all corrupted data files if any.
Use *--all* option to see statuses of all data files statuses.

The command checks:
1. Cache file name which equals to the file content checksum on the time when DVC created the file.
2. Checksum from local state file.
3. Checksum regarding DVC files.
4. Actual recomputed checksum. This is computation heavy command for large data file. Enabled only by *--physical* option.

Data file is considered as corrupted if one of the checksum does not match all others.


.. code-block:: shell
	:linenos:

	dvc fsck [-h] [-q] [-v] [-p] [-a] [targets [targets ...]]

	positional arguments:
		targets               Data files to check

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-p, --physical        Compute actual md5
		-a, --all             Show all data files including correct ones

Examples.


Check list of corrupted data files::

	$ dvc fsck --physical
	File data/matrix-test.p:
	    Error status:           Checksum missmatch!!!
	    Actual checksum:        7c4521365288d69a03fa22ad3d399f32
	    Cache file name:        7c4521365288d69a03fa22ad3d399f32
	    Local state checksum:   7c4521365288d69a03fa22ad3d399f32
	    Local state mtime:      1517048086.0
	    Actual mtime:           1517048086.0
	    Stage file: eval_auc.txt.dvc
	        Checksum:           7c4521365288d69a03fa22ad3d399f32
	        Type:               Dependency
	    Stage file: matrix-train.p.dvc
	        Checksum:           7c4521365288d69a03fa22ad3d399f32
	        Type:               Output
	        Use cache:          true

Common Arguments
===========================================

Common Options
--------------

As you can see, there are four optional arguments that are applicable to any DVC command. These are

.. code-block:: shell
	:linenos:
	
	-h, --help            show this help message and exit
	-q, --quiet           Be quiet.
	-v, --verbose         Be verbose.
	-G, --no-git-actions  Skip all git actions including reproducibility check and commits.

Although these optional arguments are pretty self-explanatory, there is a note on DVC and Git commands used together.

* If you specify *--no-git-action* option, DVC does not modify (add/commit to) Git repository, however, it can still read it - for example, run *git status* command etc.
* To see Git commands in DVC, you can set logging level to *Debug* (in **dvc.conf**) or run dvc with option *--verbose*

Number of DVC Jobs
------------------

DVC can benefit from parallel processing and multiple processors/cores available on your machine. It can spin a number of jobs to run in parallel.

The number of DVC jobs is 5 by default. In case you like to change it to any other reasonable value, you use *-j (--jobs)* option in DVC commands where it is applicable.

