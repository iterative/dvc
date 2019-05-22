.. image:: https://dvc.org/static/img/logo-github-readme.png
  :target: https://dvc.org
  :alt: DVC logo

`Website <https://dvc.org>`_
• `Docs <https://dvc.org/doc>`_
• `Twitter <https://twitter.com/iterativeai>`_
• `Chat (Community & Support) <https://dvc.org/chat>`_
• `Tutorial <https://dvc.org/doc/get-started>`_
• `Mailing List <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_

.. image:: https://travis-ci.com/iterative/dvc.svg?branch=master
  :target: https://travis-ci.com/iterative/dvc
  :alt: Travis

.. image:: https://ci.appveyor.com/api/projects/status/github/iterative/dvc?branch=master&svg=true
  :target: https://ci.appveyor.com/project/iterative/dvc/branch/master
  :alt: Windows Build

.. image:: https://codeclimate.com/github/iterative/dvc/badges/gpa.svg
  :target: https://codeclimate.com/github/iterative/dvc
  :alt: Code Climate

.. image:: https://codecov.io/gh/iterative/dvc/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/iterative/dvc
  :alt: Codecov

.. image:: https://img.shields.io/badge/patreon-donate-green.svg
  :target: https://www.patreon.com/DVCorg/overview
  :alt: Donate

|

**Data Science Version Control** or **DVC** is an **open-source** tool for data science and
machine learning projects. Key features:

#. simple **command line** Git-like experience. Does not require installing and maintaining
   any databases. Does not depend on any proprietary online services;

#. it manages and versions **datasets** and **machine learning models**. Data is saved in
   S3, Google cloud, Azure, Alibaba cloud, SSH server, HDFS or even local HDD RAID;

#. it makes projects **reproducible** and **shareable**, it helps answering question how
   the model was build;

#. it helps manage experiments with Git tags or branches and **metrics** tracking;

It aims to replace tools like Excel and Docs that are being commonly used as a knowledge repo and
a ledger for the team, ad-hoc scripts to track and move deploy different model versions, ad-hoc
data file suffixes and prefixes.

.. contents:: **Contents**
  :backlinks: none

How DVC works
=============

We encourage you to read our `Get Started <https://dvc.org/doc/get-started>`_ to better understand what DVC
is and how does it fit your scenarios.

The easiest (but not perfect!) *analogy* to describe it: DVC is Git (or Git-lfs to be precise) + ``makefiles``
made right and tailored specifically for ML and Data Science scenarios.

#. ``Git/Git-lfs`` part - DVC helps you storing and sharing data artifacts, models. It connects them with your
   Git repository.
#. ``Makefiles`` part - DVC describes how one data or model artifact was build from another data.

DVC usually runs along with Git. Git is used as usual to store and version code and DVC meta-files. DVC helps
to store data and model files seamlessly out of Git while preserving almost the same user experience as if they
were stored in Git itself. To store and share data files cache DVC supports remotes - any cloud (S3, Azure,
Google Cloud, etc) or any on-premise network storage (via SSH, for example).

.. image:: https://dvc.org/static/img/flow.gif
   :target: https://dvc.org/static/img/flow.gif
   :alt: how_dvc_works

DVC pipelines (aka computational graph) feature connects code and data together. In a very explicit way you can
specify, run, and save information that a certain command with certain dependencies needs to be run to produce
a model. See the quick start section below or check `Get Started <https://dvc.org/doc/get-started>`_ tutorial to
learn more.

Quick start
===========

Please read `Get Started <https://dvc.org/doc/get-started>`_ for the full version. Common workflow commands include:

+-----------------------------------+-------------------------------------------------------------------+
| Step                              | Command                                                           |
+===================================+===================================================================+
| Track data                        | | ``$ git add train.py``                                          |
|                                   | | ``$ dvc add images.zip``                                        |
+-----------------------------------+-------------------------------------------------------------------+
| Connect code and data by commands | | ``$ dvc run -d images.zip -o images/ unzip -q images.zip``      |
|                                   | | ``$ dvc run -d images/ -d train.py -o model.p python train.py`` |
+-----------------------------------+-------------------------------------------------------------------+
| Make changes and reproduce        | | ``$ vi train.py``                                               |
|                                   | | ``$ dvc repro model.p.dvc``                                     |
+-----------------------------------+-------------------------------------------------------------------+
| Share code                        | | ``$ git add .``                                                 |
|                                   | | ``$ git commit -m 'The baseline model'``                        |
|                                   | | ``$ git push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+
| Share data and ML models          | | ``$ dvc remote add myremote -d s3://mybucket/image_cnn``        |
|                                   | | ``$ dvc push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+

Installation
============

Read this `instruction <https://dvc.org/doc/get-started/install>`_ to get more details. There are three
options to install DVC: ``pip``, Homebrew, or an OS-specific package:

pip (PyPI)
----------

.. code-block:: bash

   pip install dvc

Depending on the remote storage type you plan to use to keep and share your data, you might need to specify
one of the optional dependencies: ``s3``, ``gs``, ``azure``, ``oss``, ``ssh``. Or ``all_remotes`` to include them all.
The command should look like this: ``pip install dvc[s3]`` - it installs the ``boto3`` library along with
DVC to support the AWS S3 storage.

To install the development version, run:

.. code-block:: bash

   pip install git+git://github.com/iterative/dvc

Homebrew
--------

.. code-block:: bash

   brew install iterative/homebrew-dvc/dvc

or:

.. code-block:: bash

   brew cask install iterative/homebrew-dvc/dvc

Package
-------

Self-contained packages for Windows, Linux, Mac are available. The latest version of the packages can be found at
GitHub `releases page <https://github.com/iterative/dvc/releases>`_.

Ubuntu / Debian (deb)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/deb/dvc.list -O /etc/apt/sources.list.d/dvc.list
   sudo apt-get update
   sudo apt-get install dvc

Fedora / CentOS (rpm)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/rpm/dvc.repo -O /etc/yum.repos.d/dvc.repo
   sudo yum update
   sudo yum install dvc

Arch Linux (AUR)
^^^^^^^^^^^^^^^^
*Unofficial package*, any inquiries regarding the AUR package,
`refer to the maintainer <https://github.com/mroutis/pkgbuilds>`_.

.. code-block:: bash

   yay -S dvc

Related technologies
====================

#. `Git-annex <https://git-annex.branchable.com/>`_ - DVC uses the idea of storing the content of large files (that you
   don't want to see in your Git repository) in a local key-value store and uses file hardlinks/symlinks instead of the
   copying actual files.

#. `Git-LFS <https://git-lfs.github.com/>`_ - DVC is compatible with any remote storage (S3, Google Cloud, Azure, SSH,
   etc). DVC utilizes reflinks or hardlinks to avoid copy operation on checkouts which makes much more efficient for
   large data files.

#. *Makefile* (and its analogues). DVC tracks dependencies (DAG).

#. `Workflow Management Systems <https://en.wikipedia.org/wiki/Workflow_management_system>`_. DVC is a workflow
   management system designed specifically to manage machine learning experiments. DVC is built on top of Git.

#. `DAGsHub <https://dagshub.com/>`_ Is a Github equivalent for DVC - pushing your Git+DVC based repo to DAGsHub will give you a high level dashboard of your project, including DVC pipeline and metrics visualizations, as well as links to DVC managed files if they are in cloud storage.

Contributing
============
Contributions are welcome! Please see our `Contributing Guide <https://dvc.org/doc/user-guide/contributing/>`_ for more
details.

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/0
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/0
  :alt: 0

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/1
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/1
  :alt: 1

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/2
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/2
  :alt: 2

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/3
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/3
  :alt: 3

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/4
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/4
  :alt: 4

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/5
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/5
  :alt: 5

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/6
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/6
  :alt: 6

.. image:: https://sourcerer.io/fame/efiop/iterative/dvc/images/7
  :target: https://sourcerer.io/fame/efiop/iterative/dvc/links/7
  :alt: 7

Mailing List
============

Want to stay up to date? Want to help improve DVC by participating in our occasional polls? Subscribe to our `mailing list <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_. No spam, really low traffic.

Copyright
=========

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request for this project, you agree to license your contribution under the Apache license version
2.0 to this project.
