====================================
Collaboration issues in data science
====================================

Even with all the successes today in machine learning (ML), specifically deep learning and its applications in business, the data science community is still lacking good practices for organizing their projects and effectively collaborating across their varied ML projects.
This is a massive challenge for the community and the industry now, when ML algorithms and methods are no longer simply "tribal knowledge" but are still difficult to implement, reuse, and manage.

To make progress in this challenge, many areas of the ML experimentation process need to be formalized. Many common questions need to be answered in an unified, principled way:

1. Source code and data versioning.

- How do you avoid any discrepancies between versions of the source code and versions of the data files when the data cannot fit into a repository?

2. Experiment time log.

- How do you track which of the hyperparameter changes contributed the most to producing your target metric? How do you monitor the extent of each change?

3. Navigating through experiments.

- How do you recover a model from last week without wasting time waiting for the model to re-train?

- How do you quickly switch between the large data source and a small data subset without modifying source code?

4. Reproducibility.

- How do you rerun a model's evaluation without re-training the model and preprocessing a raw dataset?

5. Managing and sharing large data files.

- How do you share models trained in a GPU environment with colleagues who do not have access to a GPU?

- How do you share the entire 147Gb of your project, with all of its data sources, intermediate data files, and models?


Some of these questions are easy to answer individually.
Any data scientist, engineer, or manager knows or could easily find answers to some of them.
However, the variety of answers and approaches makes data science collaboration a nightmare.
**A systematic approach is required.**

=========================
Tools for data scientists
=========================

Existing engineering tools
__________________________

There is one common opinion regarding data science tooling.
Data scientists as engineers are supposed to use the best practices and collaboration software from software engineering. 
Source code version control system (Git), continuous integration services (CI), and unit test frameworks are all expected to be utilized in the data science pipeline.

But a comprehensive look at data science processes shows that the software engineering toolset does not cover data science needs. Try to answer all the questions from the above using only engineering tools, and you are likely to be left wanting for more.

Experiment management software
______________________________

To solve data scientists collaboration issues a new type of software was created - **experiment management software**.
This software aims to cover the gap between data scientist needs and the existing toolset.

The experimentation software is usually **user interface (UI) based in contrast to the existing command line engineering tools**.
The UI is a bridge to a **separate cloud based environment**.
The cloud environment is usually not so fixible as local data scientists environement.
And the cloud environment is not fully integrated with the local environment.

The separation of the local data scientist environment and the experimentation cloud environment creates another discrepancy issue and the environment synchronization requires addition work.
Also, this style of software usually require external services, typically accompanied with a monthly bill.
This might be a good solution for a particular companies or groups of data scientists.
However a more accessible, free tool is needed for a wider audience.

============
What is DVC?
============

Data Version Control, or DVC, is **a new type of experiment management software** that has been built **on top of the existing engineering toolset** and particularly on a source code version control system (currently - Git).
DVC reduces the gap between the existing tools and the data scientist needs.
This gives an ability to **use the advantages of the experimentation software while reusing existing skills and intuition**.

The underlying source code control system **eliminates the need to use external services**.
Data science experiment sharing and data scientist collaboration can be done through regular Git tools (commit messages, merges, pull requests, code comments), the same way it works for software engineers.

DVC implements a **Git experimentation methodology** where each experiment exists with its code as well as data, and can be represented as a separate Git branch or commit.

DVC uses a few core concepts:

- **Experiment** is equivalent to a Git branch. Each experiment (extract new features, change model hyperparameters, data cleaning, add a new data source) should be performed in a separate branch and then merged into the master branch only if the experiment is successful. DVC allows experiments to be integrated into a project's history and NEVER needs to recompute the results after a successful merge.

- **Experiment state** or state is equivalent to a Git snapshot (all committed files). Git checksum, branch name, or tag can be used as a reference to a experiment state.

- **Reproducibility** - an action to reproduce an experiment state. This action generates output files based on a set of input files and source code. This action usually changes experiment state.

- **Pipeline** - directed acyclic graph (DAG) of commands to reproduce an experiment state. The commands are connected by input and output files. Pipeline is defined by special **dvc-files** (which act like Makefiles).

- **Workflow** - set of experiments and relationships among them. Workflow corresponds to the entire Git repository.

- **Data files** - cached files (for large files). For data files the file content is stored outside of the Git repository on a local hard drive, but data file metadata is stored in Git for DVC needs (to maintain pipelines and reproducibility).

- **Data cache** - directory with all data files on a local hard drive or in cloud storage, but not in the Git repository.

- **Cloud storage** support is a compliment to the core DVC features. This is how a data scientist transfers large data files or shares a trained on GPU model to whom who does not have GPU.


=============
Core features
=============

1. DVC works **on top of Git repositories** and has a similar command line interface and Git workflow.

2. It makes data science projects **reproducible** by creating lightweight pipelines of DAGs.

3. **Large data file versioning** works by creating pointers in your Git repository to the data cache on a local hard drive.

4. **Programming language agnostic**: Python, R, Julia, shell scripts, etc. ML library agnostic: Keras, Tensorflow, PyTorch, scipy, etc.

5. **Open-sourced** and **Self-served**. DVC is free and does not require any additional services.

6. DVC supports cloud storage (AWS S3 and GCP storage) for **data sources and pre-trained models sharing**.


====================
Related technologies
====================

Due to the the novelty of this approach, DVC can be better understood in comparison to existing technologies and ideas.

DVC combines a number of existing technologies and ideas into a single product with the goal of bringing the best engineering practices into the data science process.

1. **Git**. The difference is:

   - DVC extends Git by introducing the concept of *data files* - large files that should NOT be stored in a Git repository but still need to be tracked and versioned.

2. **Workflow management tools** (pipelines and DAGs): Apache Airflow, Luigi and etc. The differences are:

   - DVC is focused on data science and modeling. As a result, DVC pipelines are lightweight, easy to create and modify. However, DVC lacks pipeline execution features like execution monitoring, execution error handling, and recovering.

   - DVC is purely a command line tool that does not have a user interface and does not run any servers. Nevertheless, DVC can generate images with pipeline and experiment workflow visualization.

3. **Experiment management** software today is mostly designed for enterprise usage. An open-sourced experimentation tool example: http://studio.ml/. The differences are:

   - DVC uses Git as the underlying platform for experiment tracking instead of a web application.

   - DVC does not need to run any services. No user interface as a result, but we expect some UI services will be created on top of DVC.

   - DVC has transparent design: DVC-files, meta files, state file, cache dirs have a simple format and can be easily reused by external tools.

4. **Git workflows** and Git usage methodologies such as Gitflow. The differences are:

   - DVC supports a new experimentation methodology that integrates easily with a Git workflow. A separate branch should be created for each experiment, with a subsequent merge of this branch if it was successful.

   - DVC innovates by allowing experimenters the ability to easily navigate through past experiments without recomputing them.


5. **Makefile** (and it's analogues). The differences are:

   - DVC utilizes a DAG:

     - The DAG is defined by dvc-files with filenames *Dvcfile* or *<filename>.dvc*.

     - One dvc-file defines one node in the DAG. All dvc-files in a repository make up a single pipeline (think a single Makefile). All dvc-files (and corresponding pipeline commands) are implicitly combined through their inputs and outputs, to simplify conflict resolving during merges.

     - DVC provides a simple command *dvc run CMD* to generate a dvc-file automatically based on the provided command, dependencies, and outputs.

   - File tracking:

     - DVC tracks files based on checksum (md5) instead of file timestamps. This helps avoid running into heavy processes like model re-training when you checkout a previous, trained version of a modeling code (Makefile will retrain the model).

     - DVC uses the files timestamps and inodes for optimization. This allows DVC to avoid recomputing all dependency files checksum, which would be highly problematic when working with large files (10Gb+).


6. **Git-annex**. The differences are:

   - DVC uses the idea of storing the content of large files (that you don't want to see in your Git repository) in a local key-value store and use file symlinks instead of the actual files.

   - DVC uses hardlinks instead of symlinks to make user experience better.

   - DVC optimizes checksum calculation.

   - DVC stores data file metadata in Git repository *.dvc/*, not in the Git tree *.git/annex/*. As a result, all metadata can be shared through any Git server like Github (Git-annex loses all metadata when shared by Git server).

7. **Git-LFS** (Large File Storage). The differences are:

   - DVC is fully compatible with Git. It does not require special Git servers like Git-LFS demands.

   - DVC does not add any hooks to Git by default. To checkout data files, the *dvc checkout* command has to be run after each *git checkout* and *git clone* command.

   - DVC creates hardlinks instead and changes data file permissions to read only. The *dvc checkout* command does not actually copy data files from cache to the working tree, as copying files is a heavy operation for large files (30Gb+).


=================
How does it work?
=================

1. DVC is a command line tool that works on top of Git::

	$ cd my_git_repo
	$ dvc init

2. DVC helps define pipelines of your commands, and keeps all the commands and dependencies in a Git repository::

	$ dvc run -d input.csv -o results.csv python cnn_train.py --seed 20180227 --epoch 20 input.csv result.csv
	$ git add results.csv.dvc
	$ git commit -m 'Train CNN. 20 epochs.'

3. DVC is programming language agnostic. R command example::

	$ dvc run -d result.csv -o plots.jpg Rscript plot.R result.csv plots.jpg
	$ git add plots.jpg.dvc
	$ git commit -m 'CNN plots'

4. DVC can reproduce a pipeline with respect to the pipeline's dependencies::

	# The input dataset was changed
	$ dvc repro plots.jpg.dvc
	Reproducing 'output.p':
	    python cnn_train.py --seed 20180227 --epoch 20 input.csv output.p
	Reproducing 'plots.jpg':
	    Rscript plot.R result.csv plots.jpg

5. DVC introduces the concept of data files to Git repositories. DVC keeps data files outside of the repository but retains the metadata in Git::

	$ git checkout a03_normbatch_vgg16 # checkout code and DVC meta data
	$ dvc checkout # checkout data files from the local cache (not Git)
	$ ls -l data/ # These LARGE files were copied from DVC cache, not from Git
	total 1017488
	-r--------  2 501  staff   273M Jan 27 03:48 Posts-test.tsv
	-r--------  2 501  staff    12G Jan 27 03:48 Posts-train.tsv


6. DVC makes repositories reproducible. DVC metadata can be easily shared through any Git server, and allows for experiments to be easily reproduced::

	$ git clone https://github.com/dataversioncontrol/myrepo.git
	$ cd myrepo
	# Reproduce data files
	$ dvc repro
	Reproducing 'output.p':
	    python cnn_train.py --seed 20180227 --epoch 20 input.csv output.p
	Reproducing 'plots.jpg':
	    Rscript plot.R result.csv plots.jpg

7. DVC's local cache can be transferred to your colleagues and partners through AWS S3 or GCP Storage::

	$ git push
	$ dvc push # push the data cache to your cloud bucket

	# On a colleague machine:
	$ git clone https://github.com/dataversioncontrol/myrepo.git
	$ cd myrepo
	$ git pull # get the data cache from cloud
	$ dvc checkout # checkout data files
	$ ls -l data/ # You just got gigabytes of data through Git and DVC:
	total 1017488
	-r--------  2 501  staff   273M Jan 27 03:48 Posts-test.tsv

8. DVC works on Mac, Linux ,and Windows. A Windows example::

	$ dir
	?????
	????


==============================
Getting Started with DVC
==============================

To show DVC in action, let's play with an actual machine learning scenario.
Let's explore the natural language processing (NLP) problem of predicting tags for a given
    StackOverflow question.
For example, we want one classifier which can predict a post that is about the Java language by tagging it "Java".

First, let's download the model code and set up the Git repository::

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

The full pipeline can be built by running the code below::

	$ # Initialize DVC repository (in your Git repository)
	$ dvc init

	$ # Download a file and put it into the data/ directory.
	$ dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/

	$ # Extract XML from the archive.
	$ dvc run tar zxf data/Posts.xml.tgz -C data/

	$ # Prepare the data.
	$ dvc run python code/xml_to_tsv.py data/Posts.xml data/Posts.tsv python

	$ # Split training and testing dataset. Two output files.
	$ # 0.33 is the test dataset split ratio. 20170426 is a seed for randomization.
	$ dvc run python code/split_train_test.py data/Posts.tsv 0.33 20170426 data/Posts-train.tsv data/Posts-test.tsv

	$ # Extract features from the data. Two TSV as inputs with two pickle matrices as outputs.
	$ dvc run python code/featurization.py data/Posts-train.tsv data/Posts-test.tsv data/matrix-train.p data/matrix-test.p

	$ # Train ML model on the training dataset. 20170426 is another seed value.
	$ dvc run python code/train_model.py data/matrix-train.p 20170426 data/model.p

	# Evaluate the model on the test dataset.
	$ dvc run python code/evaluate.py data/model.p data/matrix-test.p data/evaluation.txt

	$ # The result.
	$ cat data/evaluation.txt
	AUC: 0.596182

Your code can be easily reproduced after some minor modification::

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

It's easy to integrate DVC into your existing ML pipeline/processes without any significant effort to re-implement your code/application.

The key step to note is that DVC automatically derives the dependencies between the experiment steps and builds the dependency graph (DAG) transparently.

Not only can DVC streamline your work into a single, reproducible environment, it also makes it easy to share this environment by Git including the dependencies  —  an exciting collaboration feature which gives the ability to reproduce the research easily in a myriad of environments.


============
Installation
============

Operating system dependent packages are the recommended way to install DVC.
Some other methods of installation are available.

OS packages
===========

DVC installation packages are available for Mac OS, Linux, and Windows.
You can download the packages here at https://github.com/dataversioncontrol/dvc/releases/

Python pip
==========

Another option is to use the standard Python pip package::

	$ pip install dvc

**Note:** if you use *Anaconda*, it will work in *Anaconda’s* command prompt tool.
At the moment, DVC does not provide a special installation package for the native *Anaconda* package manager *conda*.

Homebrew Cask
=============

Mac OS users can install DVC by using the **brew** command::

	$ brew cask install dataversioncontrol/homebrew-dvc/dvc

Development Version
===================

If you would like to pull the latest version of DVC, you can do the following::

	$ pip install git+git://github.com/dataversioncontrol/dvc

Note: this will automatically upgrade your DVC version to the latest version.


=============
Configuration
=============

Once you install DVC, you will be able to start using it (in its local setup) immediately.

However, you can proceed to configure DVC (especially if you intend to use it in a *cloud-based* scenario).

DVC Files and Directories
=========================

Once installed, DVC will populate its installation folder (hereafter referred to as .dvc)

* **.dvc/config** - This is a configuration file.
  The config file can be edited directly using command **dvc config NAME VALUE**.
* **.dvc/cache** - the cache directory will contain your data files (the data directories of DVC repositories will only contain hardlinks to the data files in the global cache).
  **Note:** DVC includes the cache directory to **.gitignore** file during the initialization. And no data files (with actual content) will ever be pushed to Git repository,
  only dvc-files are needed to reproduce them.
* **.dvc/state** - this file is created for optimization. The file contains data files checksum, timestamps, inodes, etc.


Working with Cloud Data Storages
======================================================

Using DVC with Cloud-based data storage is optional.
By default, DVC is configured to use a local data storage only (.dvc/cache directory),
  which enables basic DVC usage scenarios out of the box.

DVC can use cloud storage as a common file storage.
With cloud storage, you might use models and data files which were created by your team members
  without spending time and resources to re-build models and re-process data files.

As of this version, DVC supports two types of cloud-based storage providers:

* **AWS** - Amazon Web Services
* **GCP** - Google Cloud Platform

The subsections below explain how to configure DVC to use each of them.

Using AWS
---------------

To use AWS as cloud storage for your DVC repositories, you should update these **.dvc/config** options

* **Cloud = AWS** in *Global* section.
* **StoragePath = /mybucket/dvc/tag_classifier** in **AWS** section - path to a cloud storage bucket and directory in the bucket.
* **CredentialPath = ~/aws/credentials** in **AWS** section - path to AWS credentials in your local machine (AWS cli command line tools creates this directory).
  In Mac, default value is *~/.aws/credentials*, and it is *%USERPATH%/.aws/credentials* in Windows


**Important:** do not forget to commit the config file change to Git: **git commit -am "Change cloud to AWS"**

Instead of manual file modification, we recommend you run the following commands::

	$ dvc config Global.Cloud AWS # This step is not needed for new DVC repositories
	$ dvc config AWS.StoragePath /mybucket/dvc/tag_classifier
	$ dvc config AWS.CredentialPath ~/.aws/credentials # Not needed if AWS CLI is installed to default path
	$ dvc config AWS.CredentialSection default # Not needed if you have only one AWS account
	$ git commit -am "Change cloud to AWS"


Using Google Cloud
------------------

For using GCP (Google Cloud Platform) as cloud storage for your DVC repositories, you should update these **.dvc/config** options

*  **Cloud = GCP** in *Global* section.
* **StoragePath = /mybucket/dvc/tag_classifier** in GCP section - Run **dvc config GCP.StoragePath /my/path/to/a/bucket**
* **ProjectName = MyCloud** - a GCP specific project name.

**Important:** do not forget to commit the config file change to Git: **git commit -am "Change cloud to GCP"**

Instead of manual file modification, we recommend you run the following commands::

	$ dvc config Global.Cloud GCP
	$ dvc config GCP.StoragePath /mybucket/dvc/tag_classifier
	$ dvc config GCP.ProjectName MyCloud
	$ git commit -am "Change cloud to AWS"


==================
Using DVC Commands
==================

DVC is a command-line tool.
The typical use case for DVC goes as follows

* In an existing Git repository, initialize a DVC repository with **dvc init**.
* Copy source files for modeling into the repository and convert the files into DVC data files with **dvc add** command.
* Process source data files through your data processing and modeling code using the **dvc run** command.
* Use **--outs** option to specify **dvc run** command outputs which will be converted to DVC data files after the code runs.
* Clone a git repo with the code of your ML application pipeline. However, this will not copy your DVC cache. Use cloud storage settings and **dvc push** to share the cache (data).
* Use **dvc repro** to quickly reproduce your pipeline on a new iteration, after your data item files or source code of your ML application are modified.

========================
DVC Commands Cheat Sheet
========================

Below is the quick summary of the most important commands

* **dvc -h** - Show how to use DVC and show the list of commands.
* **dvc CMD -h** - Display help to use a specific DVC command (CMD).
* **dvc init** - Initialize a new DVC repository.
* **dvc add** - Add data file or data directory. The command converts regular files to DVC data files.
* **dvc checkout** - Checkout data files and dirs into the working tree. The command should be executed after **git checkout** or cloning a repository.
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

The command does the conversion from a *regular file* to *DVC data file* in a few steps:

1. Calculate the file checksum.
2. Create a cache file in the cache dir *.dvc/cache*.
3. Create a corresponding DVC file.
4. Replace the file with a hardlink to the cache file.

DVC stores the file's last modification timestamp, inode, and the checksum into a global state file *.dvc/state* to reduce time recomputing checksums later.

Note, this command does NOT copy any file contents and will run quickly even for a large files.
Step (2) from the above is also made by hardlinks movement, not file content.
The only heavy step is (1),  which requires checksum calculation.

For directories, the command does the same steps for each file recursively.
To retain information about the directory structure, a corresponding directory will be created in *.dvc/cache*.

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

Note, DVC files are created.


checkout
========

Checkout data files from cache.
This command has to be called after *git checkout* since Git does not handle DVC data files.

The command restores data files from cache to the working tree and removes data files that are no longer on the working tree.

Note, this command does NOT copy any files - DVC uses hardlinks to perform data file restoration.
This is crucial for large files where checking out as a 50Gb file might take a few minutes.
For DVC, it will take less than a second to restore a 50Gb data file.


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
However, it reports removed files and files that DVC was unable to restore due to missing cache.
To restore a file with a missing cache, the reproduction command should be called or the cache can be pulled from the cloud.

It might be convenient to assign Git hook to *git checkout*::

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

By default, stage file name is **<file>.dvc** where **<file>** is file name of the first output.

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

Execute a Python script as the DVC pipeline step. Stage file was not specified, so a **model.p.dvc** stage file will be created::

	$ # Train ML model on the training dataset. 20180226 is a seed value.
	$ dvc run -d matrix-train.p -d train_model.py -o model.p python train_model.py matrix-train.p 20180226 model.p


Execute an R script as the DVC pipeline step::

	$ dvc run -d parsingxml.R -d Posts.xml -o Posts.csv Rscript parsingxml.R Posts.xml Posts.csv


Extract an XML file from an archive to the data/ subfolder::

	$ mkdir data
	$ dvc run -d Posts.xml.tgz -o data/Posts.xml tar zxf Posts.xml.tgz -C data/


push
====

This command pushes all data file caches related to the current Git branch to cloud storage.
Cloud storage settings need to be configured.
See cloud storage configuration for more details on how to set up cloud storage.

.. code-block:: shell
   :linenos:

	usage: dvc push [-h] [-q] [-v] [-j JOBS]

	optional arguments:
	  -h, --help            show this help message and exit
	  -q, --quiet           Be quiet.
	  -v, --verbose         Be verbose.
	  -j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Push all data file caches from the current Git branch to cloud::

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

This command pulls all data file caches from cloud storage.
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
However, DVC files can have any name followed by the **.dvc** suffix.

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

Reproduce the part of the pipeline where *Posts.tsv.dvc* is the target DVC file::

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

This command collects the garbage, removing unused cache files based on the current Git branch.
If a data file was created in a different branch, then it will be removed by gc.
If a data file has a few versions (and, of course. corresponding caches) - all caches except the current one will be removed.

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

Get or set config options. This command reads and overwrites the DVC config file *.dvc/config*.


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

Specify an option name to get the option's value from config file::
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
By default the commands outputs statuses of all corrupted data files (if any).
Use *--all* option to see statuses of all data files.

The command checks:
1. Cache file name which is equal to the file content checksum when DVC created the file.
2. Checksum from local state file.
3. Checksum regarding DVC files.
4. The actual recomputed checksum. This is a computation heavy command for large data files. Enabled only by *--physical* option.

Data file is considered to be corrupted if one of the checksums does not match all others.


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

As you can see, there are four optional arguments that are applicable to any DVC command. They are

.. code-block:: shell
	:linenos:

	-h, --help            show this help message and exit
	-q, --quiet           Be quiet.
	-v, --verbose         Be verbose.

Although these optional arguments are pretty self-explanatory, there is a note for DVC and Git commands that are used together.

* To see Git commands in DVC, you can set logging level to *Debug* (in **dvc.conf**) or run dvc with option *--verbose*

Number of DVC Jobs
------------------

DVC can benefit from parallel processing and multiple processors/cores.

The number of DVC jobs is 5 by default. If you would like to change it to any other reasonable value, you use *-j (--jobs)* option in DVC commands where applicable.
