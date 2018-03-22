.. image:: https://img.shields.io/travis/dataversioncontrol/dvc/master.svg?label=Linux%20%26%20Mac%20OS
  :target: https://travis-ci.org/dataversioncontrol/dvc

.. image:: https://img.shields.io/appveyor/ci/dataversioncontrol/dvc/master.svg?label=Windows
  :target: https://ci.appveyor.com/project/dataversioncontrol/dvc/branch/master

.. image:: https://codeclimate.com/github/dataversioncontrol/dvc/badges/gpa.svg
  :target: https://codeclimate.com/github/dataversioncontrol/dvc

.. image:: https://codecov.io/gh/dataversioncontrol/dvc/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/dataversioncontrol/dvc

Data Version Control or DVC is an open source tool for data science projects. 
It helps data scientists manage their code and data together in a simple form of Git-like commands.

Get started
===========
+-----------------------------------+-------------------------------------------------------------------+
| Step                              | Command                                                           |
+===================================+===================================================================+
| Track code and data together      | | ``$ git add train.py``                                          |
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
| Share data and ML models          | | ``$ dvc config AWS.StoragePath mybucket/image_cnn``             |
|                                   | | ``$ dvc push``                                                  |
+-----------------------------------+-------------------------------------------------------------------+

See more in `tutorial <https://blog.dataversioncontrol.com/data-version-control-tutorial-9146715eda46>`_.

Installation
============

Packages
--------

Operating system dependent packages are the recommended way to install DVC.
The latest version of the packages can be found at GitHub releases page: https://github.com/dataversioncontrol/dvc/releases

Python Pip
----------

DVC could be installed via the Python Package Index (PyPI).

.. code-block:: bash

   pip install dvc

Homebrew (Mac OS)
-----------------

Formula:
^^^^^^^^

.. code-block:: bash

   brew install dataversioncontrol/homebrew-dvc/dvc

Cask:
^^^^^

.. code-block:: bash

   brew cask install dataversioncontrol/homebrew-dvc/dvc

Links
=====

Website: https://dataversioncontrol.com

Tutorial: https://blog.dataversioncontrol.com/data-version-control-tutorial-9146715eda46

Documentation: http://dataversioncontrol.com/docs/

Discussion: https://discuss.dataversioncontrol.com/

Related technologies
====================


#. `Git-annex <https://git-annex.branchable.com/>`_ - DVC uses the idea of storing the content of large files (that you don't want to see in your Git repository) in a local key-value store and uses file hardlinks/symlinks instead of the copying actual files.
#. `Git-LFS <https://git-lfs.github.com/>`_.
#. Makefile (and it's analogues). DVC tracks dependencies (DAG). 
#. `Workflow Management Systems <https://en.wikipedia.org/wiki/Workflow_management_system>`_. DVC is workflow management system designed specificaly to manage machine learning experiments. DVC was built on top of Git.

DVC is compatible with Git for storing code and the dependency graph (DAG), but not data files cache.
Data files caches can be transferred separately - now data cache transfer throught AWS S3 and GCP storge are supported.

How DVC works
=============


.. image:: https://s3-us-west-2.amazonaws.com/dvc-share/images/0.9/how_dvc_works.png
   :target: https://s3-us-west-2.amazonaws.com/dvc-share/images/0.9/how_dvc_works.png
   :alt: how_dvc_works


Copyright
=========

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request for this project, you agree to license your contribution under the Apache license version 2.0 to this project.
