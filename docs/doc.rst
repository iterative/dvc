========================
Introduction
========================

It is hardly possible in real life to develop a good machine learning model in a single pass. ML modeling is an iterative process and it is extremely important to keep track of your steps, dependencies between the steps, dependencies between your code and data files and all code running arguments. This becomes even more important and complicated in a team environment where data scientists’ collaboration takes a serious amount of the team’s effort.

**Data Version Control** (aka DVC) is designed to help data scientists keep track of their ML processes and file dependencies in the simple form of git-like commands: "dvc run python train_model.py data/train_matrix.p data/model.p". Your existing ML processes can be easily transformed into reproducible DVC pipelines regardless of which programming language or tool was used.

==================
DVC Basic
==================

Traditional Makefile based reproducibility
__________________________________________

Traditional approach in software enginering - reproduce everything from code.
This is nice and clean way to build software from source code and it is heavely utilised by Makefile and it's analogs.
The approach is based on some implicit assumptions:

* Project consists on a large amount of small source code files
* Each code file can be processed (compiled) separately into some object file
* The final result (an application file) is a combination of these object files
* It is easy derive what was changed from the last reproduction (last make run)
* If only a few files were changed it is easy to rebuild only this subset of files and build a final result.

Makefile tool and it's analogs do a good job in recognizing the small changes (step 4), rebuilding small parts of the project and constrcting them together into a single result.


Data science project
____________________


Two reproducibility philosophies
________________________________



There are two different reproducibility "philosophies":
* Versioning only code. 
* Versioning code and data.

Basic assumption: Data and object files can be easily derived from code.



______________________
DVC is a command line tool that works on top of an existing Git repository.

Using traditional UNIX-tool terminology DVC might be treated as a Makefile for data projects which
1. DVC never rebuilds a target if it was already built
2. DVC 


========================
Getting Started with DVC
========================

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
However, you can install DVC from Python repositories using **pip** command or install development version from DVC git repository.

Packages
========

DVC installation packages available for Mac OS, Linux and Windows platforms.
You can download the packages at https://github.com/dataversioncontrol/dvc/releases/

Python pip
==========

Another option to deploy DVC to your machine is to use its standard Python pip package::

	$ pip install dvc

**Note:** if you use *Anaconda*, you can use the above-mentioned command there as well.
It will work in *Anaconda’s* command prompt tool.
As of the moment, DVC does not provide a special installation package for a native *Anaconda* package manager (that is, *conda*).

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

Since DVC is a command-line application, the appropriate method of use of DVC commands is essential to properly utilize it to the benefit of your machine learning projects.

The typical method of use of DVC is as follows

* You initialize a local DVC repository with **dvc init** command
* You pull or import data files into DVC repository either with **dvc pull** command or via external process invoked by **dvc run** command
* You clone a git repo with the code of your ML application pipeline 
* You execute the steps in your ML pipeline as needed (**dvc run** command is often used to run respective processes/steps of your ML pipeline)
* You use **dvc repro** command to quickly reproduce your ML pipeline on a new iteration, once either the data item files or the source code of your ML application are modified
* You push the results of calculations back to your data storage using **dvc push** command

**Note:** please refer to “Further Reading” section to see in-depth articles and tutorials on the end-to-end ML pipeline setup with DVC for real-world ML projects in Python and R.

========================
DVC Commands Cheat Sheet
========================

Below is the quick summary of the most important commands of DVC

* **dvc -h** - Show how to use DVC and show the list of commands.
* **dvc CMD -h** - Display help to use a specific DVC command (CMD)
* **dvc init** - Initialize a new local DVC repository in a folder of your choice (this folder should be already initialized either as a local git repository or a clone of a remote git repository)
* **dvc run** - Run an external command (for example, launch Python runtime with a python script to execute as its argument)
* **dvc pull** - Pull data files from the cloud (cloud settings for your DVC environment should be already configured prior to using this command).
* **dvc push** - Push data files to the cloud (cloud settings for your DVC environment should be already configured prior to using this command).
* **dvc status** - Show status of a data file in the DVC repository
* **dvc repro** - Reproduce the entire ML pipeline (or its part) where affected changes relate to the arguments passed (for example, rerun machine learning models where a changed data file is used as an input)
* **dvc remove** - Remove data items (files or/and folders) from the local DVC repository storage
* **dvc import** - Import a data file into a local DVC repository
* **dvc lock** - Lock files in the DVC repository
* **dvc gc** - Do garbage collection and clear DVC cache
* **dvc target** - Set default target
* **dvc ex** - Execute experimental commands supported by DVC
* **dvc config** - Alter configuration settings of the DVC repository (as specified in dvc.conf) for the current session
* **dvc show** - Show graphs.

=====================
DVC Command Reference
=====================

init
====

This command initializes a local DVC environment (repository) in a current Git repository.

.. code-block:: shell
   :linenos:

	usage:

	dvc init [-h] [-q] [-v] [-G] [--data-dir DATA_DIR]
		[--cache-dir CACHE_DIR] [--state-dir STATE_DIR] 

	optional arguments:
		-h, --help             show this help message and exit
		-q, --quiet            Be quiet.
		-v, --verbose          Be verbose.
		-G, --no-git-actions   Skip all git actions including reproducibility check and commits.
		--data-dir DATA_DIR    Data directory.
		--cache-dir CACHE_DIR  A well-formed path to the Cache directory.
		--state-dir STATE_DIR  A well-formed path to the State directory.

Example.  Creating a new Git repository and DVC::

	$ mkdir tag_classifier
	$ cd tag_classifier
	
	$ git init
	Initialized empty Git repository in /Users/dmitry/src/tag_classifier__3/.git/
	
	$ dvc init
	Directories .dvc/, data/, cache/, state/ were created
	File .gitignore was created
	Directory cache was added to .gitignore file


run
===

This command executes is used to execute the steps in your ML pipeline, for instance
* Running a python or R script
* Running a database SQL script
* Etc.

.. code-block:: shell
   :linenos:
   
	usage: dvc run [-h] [-q] [-v] [-G] [--stdout STDOUT] [--stderr STDERR]
               [-i INPUT] [-o OUTPUT] [-c CODE] [--shell] [-l]
               command [args]

	positional arguments:
		command     Command to execute
		args        Arguments of a command (optional; it can be a list of the command-line arguments of command separated by spaces)

	optional arguments:
		-h, --help                   show this help message and exit
		-q, --quiet                  Be quiet.
		-v, --verbose                Be verbose.
		-G, --no-git-actions         Skip all git actions including reproducibility check and commits.
		--stdout STDOUT              Output std output to a file.
		--stderr STDERR              Output std error to a file.
		-i INPUT, --input INPUT      Declare input data items for reproducible cmd.
		-o OUTPUT, --output OUTPUT   Declare output data items for reproducible cmd.
		-c CODE, --code CODE         Code dependencies which produce the output.
		--shell                      Shell command
		-l, --lock                   Lock data item - disable reproduction.

Examples:

Execute a Python script as a DVC ML pipeline step::

	$ # Train ML model out of the training dataset. 20170426 is another seed value.
	$ dvc run python code/train_model.py data/matrix-train.p 20170426 data/model.p


Execute an R script as a DVC ML pipeline step::

	dvc run Rscript code/parsingxml.R data/Posts.xml data/Posts.csv


Extract an XML file from an archive to data subfolder::

	dvc run tar zxf data/Posts.xml.tgz -C data/


push
====

This command pushes data files to the cloud storage you configured for DVC.

.. code-block:: shell
   :linenos:

	usage: dvc push [-h] [-q] [-v] [-G] [-j JOBS] targets [targets ...]

	positional arguments:
		targets               File or directory to sync.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
							  and commits.
		-j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Push all files from the current DVC snapshot to cloud::

	$ dvc push data/

pull
====

This command pulls data from the cloud storage you configured for DVC.

.. code-block:: shell
   :linenos:
   
	usage: dvc pull [-h] [-q] [-v] [-G] [-j JOBS] targets [targets ...]

	positional arguments:
		targets               File or directory to sync.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-j JOBS, --jobs JOBS  Number of jobs to run simultaneously.


Examples:

Pull all files from the current DVC snapshot to cloud::

	$ dvc pull data/


status
======

This command shows status for data files in the DVC repository

.. code-block:: shell
	:linenos:

	usage: dvc status [-h] [-q] [-v] [-G] [-j JOBS] targets [targets ...]

	positional arguments:
		targets               File or directory to sync.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
                              and commits.
		-j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

Examples:

Get status of data in *training.csv* file::

	$ dvc status data/training.csv

repro
=====

This command reproduces the that part of the ML pipeline that is dependent on the data or code file targeted by it.

.. code-block:: shell
	:linenos:

	usage: dvc repro [-h] [-q] [-v] [-G] [-f] [-s] [target [target ...]]

	positional arguments:
		target                Data items to reproduce.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-f, --force           Reproduce even if dependencies were not changed.
		-s, --single-item     Reproduce only single data item without recursive dependencies check.

Examples:

Reproduce the part of the pipeline where *training.csv* data file is involved::

	$ dvc repro data/training.csv


remove
======

This command removes a data item from the data directory of a DVC repository.

.. code-block:: shell
	:linenos:

	usage: dvc remove [-h] [-q] [-v] [-G] [-l] [-r] [-c] [target [target ...]]

	positional arguments:
		target                Target to remove - file or directory.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
							  and commits.
		-l, --keep-in-cloud   Do not remove data from cloud.
		-r, --recursive       Remove directory recursively.
		-c, --keep-in-cache   Do not remove data from cache.

Examples:


Remove *training.csv* data file from the DVC repository::

	$ dvc remove data/training.csv

import
======

This command imports a new data file to the data directory of the DVC repository.

.. code-block:: shell
	:linenos:

	usage: dvc import [-h] [-q] [-v] [-G] [-l] [-j JOBS] [-c]
               input [input ...] output

	positional arguments:
		input             Input file/files.
		output            Output file/directory.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-l, --lock            Lock data item - disable reproduction.
		-j JOBS, --jobs JOBS  Number of jobs to run simultaneously.
		-c, --continue        Resume downloading file from url

Examples:

Download a file and put to data/ directory::

	$ dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/

lock
====

This command is used to

* Lock the data item in the DVC repository, protecting it from further changes
* Unlock the data item locked earlier (switch -u is added in this case)

.. code-block:: shell
	:linenos:

	usage: dvc lock [-h] [-q] [-v] [-G] [-l] [-u] [files [files ...]]

	positional arguments:
		files                 Data items to lock or unlock.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
							  and commits.
		-l, --lock            Lock data item - disable reproduction.
		-u, --unlock          Unlock data item - enable reproduction.

Examples.


Lock *data/Posts.xml* file::

	$ dvc lock data/Posts.xml

Unlock a previously locked *data/Posts.xml* file::

	$ dvc lock -u data/Posts.xml

**Notes**

* If you invoke lock command with *-u* switch against a locked target file, it will be unlocked
* Adding *-l* switch to any other command where *-l* switch is enabled will automatically lock/unlock the target files (much like you do with a separate lock command against that target)

gc
===
This command collects the garbage in DVC environment.
It is especially important when you work with large data files.
Under such a condition, keeping previous versions of the large files may slow down performance/drain the disk quota thus swift removing of unnecessary files will be beneficial.

.. code-block:: shell
	:linenos:
	
	usage: dvc gc [-h] [-q] [-v] [-G] [-l] [-r] [-c] [target [target ...]]

	positional arguments:
		target                Target to remove - file or directory.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-l, --keep-in-cloud   Do not remove data from cloud.
		-r, --recursive       Remove directory recursively.
		-c, --keep-in-cache   Do not remove data from cache.

Examples:

Remove all cloud not in the current DVC snapshot::

	$ dvc gc data/

Remove all versions of *data/Posts.xml* file (but the latest one) from the local cache directory but keep it in the cloud storage::

	$ dvc gc data/Posts.xml --keep-in-cloud

target
======

This command sets the default target for the current DVC repository.

.. code-block:: shell
	:linenos:
	
	usage: dvc target [-h] [-q] [-v] [-G] [-u] [target_file]

	positional arguments:
		target_file           Target data item.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-u, --unset           Reset target.

Examples:

Set *data/Posts.xml* file as a default target in the current DVC repository::

	$ dvc target data/Posts.xml

ex
==

This command is designed for risky enthusiasts who would like to try the newest capabilities of DVC which are still under active development. 

**Note:** It is provided for your reference and early try only. DVC development team does not provide any warranty as for this piece of DVC functionality to work in a stable manner, in your environment. We do not recommend you to use the experimental functionality in production mode unless you really clear as for what you are going to do.

.. code-block:: shell
	:linenos:
	
	usage: dvc ex [-h] [-q] [-v] [-G] {cloud} ...

	positional arguments:
		{cloud}             Use dvc cloud CMD --help for command-specific help
		cloud               Cloud manipulation

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
                              and commits.


config
======

This command is designed to overwrite some configuration options for just this session of DVC (as you remember, default configuration values are specified in **dvc.conf** , which is located in root of your DVC installation folder).

.. code-block:: shell
	:linenos:
	
	usage: dvc config [-h] [-q] [-v] [-G] [-u] name [value]

	positional arguments:
		name                  Option name
		value                 Option value

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
                              and commits.
		-u, --unset           Unset option

Examples:

Overwrite the value of DataDir configuration option with  *etc/data* for a current dvc session only::

	$ dvc config DataDir etc/data

show
====

This command is used to display either pipeline or workflow image for your current ML project managed by DVC

.. code-block:: shell
	:linenos:
	
	usage: dvc show [-h] [-q] [-v] [-G] {pipeline,workflow} ...

	positional arguments:
		{pipeline,workflow}   Use dvc show CMD --help for command-specific help
		pipeline              Show pipeline image
		workflow              Show workflow image

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check
                              and commits.

Examples:

Show the workflow image for the ML project in your current DVC repository::

	$ dvc show workflow

Show the pipeline image for the ML project in your current DVC repository::

	$ dvc show pipeline

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

===============
Further Reading
===============

If you are interested in more information about technical aspects of using DVC in your machine learning projects, you can review technical tutorials below

* Data Version Control beta release: iterative machine learning: https://blog.dataversioncontrol.com/data-version-control-beta-release-iterative-machine-learning-a7faf7c8be67 (it exemplifies a practical case study explaining how to use DVC in a Python-based machine learning project)
* R code and reproducible model development with DVC: https://blog.dataversioncontrol.com/r-code-and-reproducible-model-development-with-dvc-1507a0e3687b (it exemplifies a practical case study explaining how to use DVC in an R-based machine learning project)
* ML Model Ensembling with Fast Iterations: https://blog.dataversioncontrol.com/ml-model-ensembling-with-fast-iterations-91e8cad6a9b5 

If you are interested in a further reading about conceptual impact of DVC on data scientist productivity and data science-to-DevOps convergence, you are welcome to review the articles below

* How A Data Scientist Can Improve His Productivity: https://blog.dataversioncontrol.com/how-a-data-scientist-can-improve-his-productivity-730425ba4aa0
* Data Version Control in Analytical DevOps Paradigm: https://blog.dataversioncontrol.com/data-version-control-in-analytics-devops-paradigm-35a880e99133 
