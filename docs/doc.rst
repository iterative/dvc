========================
Getting Started with DVC
========================

It is hardly possible in real life to develop a good machine learning model in a single pass. ML modeling is an iterative process and it is extremely important to keep track of your steps, dependencies between the steps, dependencies between your code and data files and all code running arguments. This becomes even more important and complicated in a team environment where data scientists’ collaboration takes a serious amount of the team’s effort.

**Data Version Control** application (aka DVC) is a tool that can help you to address such challenges while increasing your every-day productivity.

DVC an open source tool for data science projects. DVC makes your data science projects reproducible by automatically building data dependency graph (DAG). Your code and the dependencies could be easily shared by Git, and data - through cloud storage (AWS S3, GCP) in a single DVC environment.

It is quite easy to integrate DVC in your existing ML pipeline/processes without any significant effort to re-implement your ML code/application.

The one thing to wrap your head around is that DVC automatically derives the dependencies between the steps and builds the dependency graph (DAG) transparently to the user. This graph is used for reproducing parts of your pipeline which were affected by recent changes.

Not only can DVC streamline your work into a single, reproducible environment, it also makes it easy to share this environment by Git including the dependencies (DAG) — an exciting collaboration feature which gives the ability to reproduce the research results in different computers. Moreover, you can share your data files through cloud storage services like AWS S3 or Google Cloud Project (GCP)  Storage since DVC does not push data files to Git repositories.

**Note:** If you are interested in reading more about practical aspects of using DVC in machine learning projects, please refer to the tutorials and articles collected in ‘Further Reading’ section. There are articles there walking you through the end-to-end implementation of DVC-based pipeline for Python- and R-based ML projects.

============
Installation
============

Sections below will walk you through the installation of DVC on your computer.

Installing DVC with an Installation Package
===========================================

There are well-formed DVC installation packages available for Mac OS, Linux and Windows platforms. You can download them at https://github.com/dataversioncontrol/dvc/releases/tag/0.8.7.2-travis-macos-release-test

Once you start the installation package, it will walk you through DVC setup. You will have to just follow the installation instructions.

Installing DVC with Python pip
==============================

Another option to deploy DVC to your machine is to use its standard Python pip package.

Pre-requisites and Dependencies
-------------------------------

Before you start installation of DVC using pip, please make sure the following software applications are installed on your machine

* Latest stable version of Python 2.7.x or Python 3.x runtime for your OS
* One of the latest versions of command-line C++ compiler and its supplementary tools for your OS

Below are OS-specific notes as for the command-line C++ compiler

* If you are on a Windows machine, you may want to check if MS Visual C++ compiler is pre-installed in your system - if it is not, you can download it from http://landinghub.visualstudio.com/visual-cpp-build-tools (you should have Visual C++ 2014 Build Tools or later deployed on your Windows machine)
* Without it pre-installed, your attempt to install DVC on Windows machine will fail with the error similar to one described in https://stackoverflow.com/questions/29846087/microsoft-visual-c-14-0-is-required-unable-to-find-vcvarsall-bat (one of DVC dependency packages requires MS Visual C++ compiler to be pre-installed)
* Non-Windows OS platforms are often shipped with the pre-installed version of the command-line C++ compiler thus you should be safe there

Installation with pip
---------------------

When you install DVC on your local machine for the first time, go to your command line prompt and type the command below::

	pip install dvc

**Note:** if you use the special data science-centric Python environment provided by *Anaconda*, you can use the above-mentioned command there as well. It will work in *Anaconda’s* command prompt tool. As of the moment, DVC does not provide a special installation package for a native *Anaconda* package manager (that is, *conda*).

Installing the Development Version of DVC
-----------------------------------------

If you like to pull the latest version of DVC from the master branch in its repo at github, you execute the following command in your command prompt::

	pip install git+git://github.com/dataversioncontrol/dvc

This command will automatically upgrade your DVC version in case it is behind the latest version in the master branch of the github repo.

**Note:** if you use the special data science-centric Python environment provided by *Anaconda*, you can use the above-mentioned command there as well. It will work in *Anaconda’s* command prompt tool. As of the moment, DVC does not provide a special installation package for a native *Anaconda* package manager (that is, *conda*).

=============
Configuration
=============

Once you install DVC, you should be able to start using it (in its local setup) immediately. 

However, you can optionally proceed to further configure DVC (especially if you intend to use it in a Cloud-based scenario).

DVC Files and Directories
=========================

Once installed, dvc will populate its installation folder (hereinafter referred to as .dvc) with essential shared and internal files and folders will be stored

* **Dvc.conf** - This is a configuration file with default global settings for DVC (to change your DVC instance options, you edit it; in particular, you will edit it if decided to use DVC in the cloud data storage setup - see below)
* **.dvc/cache** - the cache folder will contain your data files (the data directories of DVC repositories will only contain only symlinks to the data files in the global cache).
* **.dvc/state** - it will It contains DAG (direct acyclic graph) of all dependencies and history of the commands in your DVC repositories

Configuring DVC to Work with Cloud-based Data Storages
======================================================

**Note:** Using DVC with Cloud-based Data Storages is an optional feature. By default, DVC is configured to use local data storage, and it enables basic DVC usage scenarios out of the box.

If your organization or team uses a cloud storage for your data, you can leverage DVC cloud storage integration capabilities.

As of this version, DVC supports two types of cloud-based data storage providers
* AWS
* Google Cloud (GC)

The subsections below explain how to configure DVC to use of the data cloud storages above.

Using AWS as a Cloud Data Storage for DVC
=========================================

If you decide to use AWS as a data cloud storage for your DVC repositories, you should update **dvc.conf** options respectively

* Set **Cloud = AWS** in *Global* section of **dvc.conf**

In *AWS* section of **dvc.conf**, specify essential details about your AWS data storage as follows

* **StoragePath** - path to a cloud storage bucket (like /mybucket) or bucket and a directory path (/mybucket/ml/dvc/ranking)
* **CredentialPath** - path to AWS credentials in your local machine (AWS tools create this dir); In Mac, it is *~/.aws/*, and it is *%USERPATH%/.aws* in Windows
* **Region** - the valid AWS region where your AWS server instance is rolled out (for example, *us-east-1*)
* **Zone**  - the valid zone with the AWS Region where your server is located (for instance, *us-east-1a*)
* **Image** - (optional) the name of the image used to create your AWS server instance (for example, *ami-2d39803a*)
* **InstanceType** - (optional) indicate your AWS instance type (for example, *t2.nano*)
* **KeyDir** - a path to your ssh key in your local machine (for instance,  *~/.ssh*)
* **KeyName** - a name of your ssh key file (for instance, *dvc-key*)

Once you save the above-mentioned changes to dvc.conf, your instance of DVC will be ready to work with AWS as a cloud data storage.

**Note:** the current version of DVC uses cloud for data storage purposes only. It does not use it for computations.

Using Google Cloud as a Cloud Data Storage for DVC
==================================================

If you decide to use AWS as a data cloud storage for your DVC repositories, you should update dvc.conf options respectively

* Set **Cloud = GC** in *Global* section of **dvc.conf**

Specify additional values in GC section of dvc.conf as follows

* **StoragePath** - has the same meaning as AWS one above
* **ProjectName** - a GCP specific stuff(just called it GCP project name for simplicity)

Once you save the above-mentioned changes to dvc.conf, your instance of DVC will be ready to work with Google Cloud as a cloud data storage.

**Note:** the current version of DVC uses cloud for data storage purposes only. It does not use it for computations.

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

* **dvc init ARGUMENTS** - Initialize a new local DVC repository in a folder of your choice (this folder should be already initialized either as a local git repository or a clone of a remote git repository)
* **dvc run ARGUMENTS** - Run an external command (for example, launch Python runtime with a python script to execute as its argument)
* **dvc pull ARGUMENTS** - Pull data files from the cloud (cloud settings for your DVC environment should be already configured prior to using this command).
* **dvc push ARGUMENTS** - Push data files to the cloud (cloud settings for your DVC environment should be already configured prior to using this command).
* **dvc status ARGUMENTS** - Show status of a data file in the DVC repository
* **dvc repro ARGUMENTS** - Reproduce the entire ML pipeline (or its part) where affected changes relate to the arguments passed (for example, rerun machine learning models where a changed data file is used as an input)
* **dvc remove ARGUMENTS** - Remove data items (files or/and folders) from the local DVC repository storage
* **dvc import ARGUMENTS** - Import a data file into a local DVC repository
* **dvc lock ARGUMENTS** - Lock files in the DVC repository
* **dvc gc ARGUMENTS** - Do garbage collection and clear DVC cache
* **dvc target ARGUMENTS** - Set default target
* **dvc ex ARGUMENTS** - Execute experimental commands supported by DVC
* **dvc config ARGUMENTS** - Alter configuration settings of the DVC repository (as specified in dvc.conf) for the current session
* **dvc show ARGUMENTS** - Show graphs
* **dvc CMD --help** - Display help to use a specific DVC command (CMD)
* **dvc CMD -h** - Display help to use a specific DVC command (CMD)

=====================
DVC Command Reference
=====================

Init Command
============
This command initializes a local DVC environment (repository) in a local directory on your machine.

**Note:** such a directory should contain either a local git repo or a remote git repo clone.

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

Examples
---------
Initializing DVC repository in the current directory::

	dvc init

Requesting help about using dvc init command::

	dvc init -h

Run command
===========

This command executes an OS command (command-line utility) on your local machine. It is often used to execute the steps in your ML pipeline, for instance
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

Examples
--------

Get help for Run command::

	dvc run -h

Execute a Python script as a DVC ML pipeline step::

	# Train ML model out of the training dataset. 20170426 is another seed value.
	dvc run python code/train_model.py data/matrix-train.p 20170426 data/model.p

**Note:** In this example, the external command is *Python* (Python runtime). *code/train_model.py* is the Python script to be executed by Python. *data/matrix-train.p, 20170426*, and *data/model.p* are command-line arguments that are passed to code/train_model.py script.

Execute an R script as a DVC ML pipeline step::

	dvc run Rscript code/parsingxml.R data/Posts.xml data/Posts.csv

**Note:** In this example, the external command is *Rscript* (R runtime script execution utility). *code/parsingxml.R* is the R script to be executed by Rscript. *data/Posts.xml* and *data/Posts.csv* are command-line arguments that are passed to code/parsingxml.R script.

Extract an XML file from an archive to data subfolder::

	dvc run tar zxf data/Posts.xml.tgz -C data/

Sync command
============

This command synchronizes data file with the cloud storage (the cloud settings should be specified in dvc.conf prior to running this command).

.. code-block:: shell
   :linenos:
   
	usage: dvc sync [-h] [-q] [-v] [-G] [-j JOBS] targets [targets ...]

	positional arguments:
	targets               File or directory to sync.

	optional arguments:
		-h, --help            show this help message and exit
		-q, --quiet           Be quiet.
		-v, --verbose         Be verbose.
		-G, --no-git-actions  Skip all git actions including reproducibility check and commits.
		-j JOBS, --jobs JOBS  Number of jobs to run simultaneously.

**Note:** this command is deprecated and it is going to be removed in one of the next releases of DVC. Technically, running **dvc sync** is equivalent to running a sequence of **dvc pull** and **dvc push** commands.

Pull Command
============

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

Examples
--------

Get help for Pull command::

	dvc pull -h

Push Command
============

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

Examples
--------

Get help for Push command::

	dvc push -h

Status Command
==============

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

Examples
--------

Get help for Status command::

	dvc status -h

Get status of data in *training.csv* file::

	dvc status data/training.csv

Repro Command
=============

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

Examples
--------

Get help for Repro command::

	dvc repro -h

Reproduce the part of the pipeline where *training.csv* data file is involved, forcing reproduce::

	dvc repro data/training.csv -f

Remove Command
==============

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

Examples
--------

Get help for Remove command::

	dvc remove -h

Remove *training.csv* data file from the DVC repository::

	dvc remove data/training.csv

Import Command
==============

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

Examples
--------

Get help for Import command::

	dvc import -h

Download a file and put to data/ directory::

	dvc import https://s3-us-west-2.amazonaws.com/dvc-share/so/25K/Posts.xml.tgz data/

Lock Command
============

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

Examples
--------

Get help for Lock command::

	dvc lock -h

Lock *data/Posts.xml* file::

	dvc lock data/Posts.xml

Unlock a previously locked *data/Posts.xml* file::

	dvc lock -u data/Posts.xml

**Notes**

* If you invoke lock command with *-u* switch against a locked target file, it will be unlocked
* Adding *-l* switch to any other command where *-l* switch is enabled will automatically lock/unlock the target files (much like you do with a separate lock command against that target)

gc Command
==========
This command collects the garbage in DVC environment. It is especially important when you work with large data files. Under such a condition, keeping previous versions of the large files may slow down performance/drain the disk quota thus swift removing of unnecessary files will be beneficial.

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

Examples
--------

Get help for gc command::

	dvc gc -h

Remove all versions of *data/Posts.xml* file (but the latest one) from the local cache directory but keep it in the cloud storage::

	dvc gc data/Posts.xml --keep-in-cloud

Target Command
==============

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

Examples
--------
Get help for Target command::

	dvc target -h

Set *data/Posts.xml* file as a default target in the current DVC repository::

	dvc target data/Posts.xml

ex Command
==========

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

Examples
--------
Display help for experimental commands in DVC::

	dvc ex -h

Config Command
==============

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

Examples
--------

Overwrite the value of DataDir configuration option with  *etc/data* for a current dvc session only::

	dvc config DataDir etc/data

Show Command
============

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

Examples
--------

Show the workflow image for the ML project in your current DVC repository::

	dvc show workflow

Show the pipeline image for the ML project in your current DVC repository::

	dvc show pipeline

Notes on Optional Arguments to DVC Commands
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