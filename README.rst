|Banner|

`Website <https://dvc.org>`_
• `Docs <https://dvc.org/doc>`_
• `VS Code Extension <https://marketplace.visualstudio.com/items?itemName=Iterative.dvc>`_
• `Blog <http://blog.dataversioncontrol.com>`_
• `Twitter <https://twitter.com/DVCorg>`_
• `Chat (Community & Support) <https://dvc.org/chat>`_
• `Tutorial <https://dvc.org/doc/get-started>`_
• `Mailing List <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_

|CI| |Maintainability| |Coverage| |VS Code| |DOI|

|PyPI| |Packages| |Brew| |Conda| |Choco| |Snap|

|

**Data Version Control** or **DVC** is a command line tool and `VS Code Extension <https://marketplace.visualstudio.com/items?itemName=Iterative.dvc>`_ to help you develop reproducible machine learning projects:

#. **Version** your data and models. Store them in your cloud storage but keep
   their version info in your Git repo.

#. **Iterate** fast with lightweight pipelines. When you make changes, only run
   the steps impacted by those changes.

#. **Track** experiments in your local Git repo (no servers needed).

#. **Compare** any data, code, parameters, model, or performance plots

#. **Share** experiments and automatically reproduce anyone's experiment.

.. contents:: **Contents**
  :backlinks: none

How DVC works
=============

We encourage you to read our `Get Started <https://dvc.org/doc/get-started>`_ guide to better understand what DVC
is and how it can fit your scenarios.

The easiest (but not perfect!) *analogy* to describe it: DVC is Git (or Git-LFS to be precise) & Makefiles
made right and tailored specifically for ML and Data Science scenarios.

#. ``Git/Git-LFS`` part - DVC helps store and share data artifacts and models, connecting them with a Git repository.
#. ``Makefile``\ s part - DVC describes how one data or model artifact was built from other data and code.

DVC usually runs along with Git. Git is used as usual to store and version code (including DVC meta-files). DVC helps
to store data and model files seamlessly out of Git, while preserving almost the same user experience as if they
were stored in Git itself. To store and share the data cache, DVC supports multiple remotes - any cloud (S3, Azure,
Google Cloud, etc) or any on-premise network storage (via SSH, for example).

|Flowchart|

The DVC pipelines (computational graph) feature connects code and data together. It is possible to explicitly
specify all steps required to produce a model: input dependencies including data, commands to run,
and output information to be saved. See the quick start sections below or
the `Get Started <https://dvc.org/doc/get-started>`_ tutorial to learn more.

Quick start
===========

Please read `Get Started <https://dvc.org/doc/get-started>`_ guide for a full version. Common workflow commands include:

+-----------------------------------+----------------------------------------------------------------------------+
| Step                              | Command                                                                    |
+===================================+============================================================================+
| Track data                        | | ``$ git add train.py``                                                   |
|                                   | | ``$ dvc add images.zip``                                                 |
+-----------------------------------+----------------------------------------------------------------------------+
| Connect code and data by commands | | ``$ dvc run -n prepare -d images.zip -o images/ unzip -q images.zip``    |
|                                   | | ``$ dvc run -n train -d images/ -d train.py -o model.p python train.py`` |
+-----------------------------------+----------------------------------------------------------------------------+
| Make changes and reproduce        | | ``$ vi train.py``                                                        |
|                                   | | ``$ dvc repro model.p.dvc``                                              |
+-----------------------------------+----------------------------------------------------------------------------+
| Share code                        | | ``$ git add .``                                                          |
|                                   | | ``$ git commit -m 'The baseline model'``                                 |
|                                   | | ``$ git push``                                                           |
+-----------------------------------+----------------------------------------------------------------------------+
| Share data and ML models          | | ``$ dvc remote add myremote -d s3://mybucket/image_cnn``                 |
|                                   | | ``$ dvc push``                                                           |
+-----------------------------------+----------------------------------------------------------------------------+

Visual Studio Code Extension
============================

|VS Code|

To get use DVC as a GUI right from your VS Code IDE, install the `DVC Extension <https://marketplace.visualstudio.com/items?itemName=Iterative.dvc>`_ from the Marketplace.
It currently features experiment tracking and data management, and more features (data pipeline support, etc.) are coming soon!

|VS Code Extension Overview|

    Note: You'll have to install core DVC on your system separately (as detailed
    below). The Extension will guide you if needed.

Installation
============

There are several ways to install DVC: in VS Code; using ``snap``, ``choco``, ``brew``, ``conda``, ``pip``; or with an OS-specific package.
Full instructions are `available here <https://dvc.org/doc/get-started/install>`_.

Snapcraft (Linux)
-----------------

|Snap|

.. code-block:: bash

   snap install dvc --classic

This corresponds to the latest tagged release.
Add ``--beta`` for the latest tagged release candidate,
or ``--edge`` for the latest ``main`` version.

Choco (Chocolatey/Windows)
--------------------------

|Choco|

.. code-block:: bash

   choco install dvc

Brew (Homebrew/Mac OS)
----------------------

|Brew|

.. code-block:: bash

   brew install dvc

Conda (Anaconda)
----------------

|Conda|

.. code-block:: bash

   conda install -c conda-forge mamba # installs much faster than conda
   mamba install -c conda-forge dvc

Depending on the remote storage type you plan to use to keep and share your data, you might need to
install optional dependencies: `dvc-s3`, `dvc-azure`, `dvc-gdrive`, `dvc-gs`, `dvc-oss`, `dvc-ssh`.

pip (PyPI)
----------

|PyPI|

.. code-block:: bash

   pip install dvc

Depending on the remote storage type you plan to use to keep and share your data, you might need to specify
one of the optional dependencies: ``s3``, ``gs``, ``azure``, ``oss``, ``ssh``. Or ``all`` to include them all.
The command should look like this: ``pip install dvc[s3]`` (in this case AWS S3 dependencies such as ``boto3``
will be installed automatically).

To install the development version, run:

.. code-block:: bash

   pip install git+git://github.com/iterative/dvc

Package
-------

|Packages|

Self-contained packages for Linux, Windows, and Mac are available. The latest version of the packages
can be found on the GitHub `releases page <https://github.com/iterative/dvc/releases>`_.

Ubuntu / Debian (deb)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/deb/dvc.list -O /etc/apt/sources.list.d/dvc.list
   wget -qO - https://dvc.org/deb/iterative.asc | sudo apt-key add -
   sudo apt update
   sudo apt install dvc

Fedora / CentOS (rpm)
^^^^^^^^^^^^^^^^^^^^^
.. code-block:: bash

   sudo wget https://dvc.org/rpm/dvc.repo -O /etc/yum.repos.d/dvc.repo
   sudo rpm --import https://dvc.org/rpm/iterative.asc
   sudo yum update
   sudo yum install dvc

Comparison to related technologies
==================================

#. Data Engineering tools such as `AirFlow <https://airflow.apache.org/>`_,
   `Luigi <https://github.com/spotify/luigi>`_, and others - in DVC data,
   model and ML pipelines represent a single ML project focused on data
   scientists' experience.  Data engineering tools orchestrate multiple data
   projects and focus on efficient execution. A DVC project can be used from
   existing data pipelines as a single execution step.

#. `Git-annex <https://git-annex.branchable.com/>`_ - DVC uses the idea of storing the content of large files (which should
   not be in a Git repository) in a local key-value store, and uses file hardlinks/symlinks instead of
   copying/duplicating files.

#. `Git-LFS <https://git-lfs.github.com/>`_ - DVC is compatible with many
   remote storage services (S3, Google Cloud, Azure, SSH, etc). DVC also
   uses reflinks or hardlinks to avoid copy operations on checkouts; thus
   handling large data files much more efficiently.

#. Makefile (and analogues including ad-hoc scripts) - DVC tracks
   dependencies (in a directed acyclic graph).

#. `Workflow Management Systems <https://en.wikipedia.org/wiki/Workflow_management_system>`_ - DVC is a workflow
   management system designed specifically to manage machine learning experiments. DVC is built on top of Git.

#. `DAGsHub <https://dagshub.com/>`_ - online service to host DVC
   projects.  It provides a useful UI around DVC repositories and integrates
   other tools.

#. `DVC Studio <https://studio.iterative.ai/>`_ - official online
   platform for DVC projects.  It can be used to manage data and models, run
   and track experiments, and visualize and share results.  Also, it
   integrates with `CML (CI/CD for ML) <https://cml.dev/>`__ for training
   models in the cloud or Kubernetes.


Contributing
============

|Maintainability|

Contributions are welcome! Please see our `Contributing Guide <https://dvc.org/doc/user-guide/contributing/core>`_ for more
details. Thanks to all our contributors!

|Contribs|

Mailing List
============

Want to stay up to date? Want to help improve DVC by participating in our occasional polls? Subscribe to our `mailing list <https://sweedom.us10.list-manage.com/subscribe/post?u=a08bf93caae4063c4e6a351f6&id=24c0ecc49a>`_. No spam, really low traffic.

Copyright
=========

This project is distributed under the Apache license version 2.0 (see the LICENSE file in the project root).

By submitting a pull request to this project, you agree to license your contribution under the Apache license version
2.0 to this project.

Citation
========

|DOI|

Iterative, *DVC: Data Version Control - Git for Data & Models* (2020)
`DOI:10.5281/zenodo.012345 <https://doi.org/10.5281/zenodo.3677553>`_.

Barrak, A., Eghan, E.E. and Adams, B. `On the Co-evolution of ML Pipelines and Source Code - Empirical Study of DVC Projects <https://mcis.cs.queensu.ca/publications/2021/saner.pdf>`_ , in Proceedings of the 28th IEEE International Conference on Software Analysis, Evolution, and Reengineering, SANER 2021. Hawaii, USA.


.. |Banner| image:: https://dvc.org/img/logo-github-readme.png
   :target: https://dvc.org
   :alt: DVC logo

.. |VS Code Extension Overview| image:: https://raw.githubusercontent.com/iterative/vscode-dvc/main/extension/docs/overview.gif
   :alt: DVC Extension for VS Code

.. |CI| image:: https://github.com/iterative/dvc/workflows/Tests/badge.svg?branch=main
   :target: https://github.com/iterative/dvc/actions
   :alt: GHA Tests

.. |Maintainability| image:: https://codeclimate.com/github/iterative/dvc/badges/gpa.svg
   :target: https://codeclimate.com/github/iterative/dvc
   :alt: Code Climate

.. |Coverage| image:: https://codecov.io/gh/iterative/dvc/branch/main/graph/badge.svg
   :target: https://codecov.io/gh/iterative/dvc
   :alt: Codecov

.. |Snap| image:: https://img.shields.io/badge/snap-install-82BEA0.svg?logo=snapcraft
   :target: https://snapcraft.io/dvc
   :alt: Snapcraft

.. |Choco| image:: https://img.shields.io/chocolatey/v/dvc?label=choco
   :target: https://chocolatey.org/packages/dvc
   :alt: Chocolatey

.. |Brew| image:: https://img.shields.io/homebrew/v/dvc?label=brew
   :target: https://formulae.brew.sh/formula/dvc
   :alt: Homebrew

.. |Conda| image:: https://img.shields.io/conda/v/conda-forge/dvc.svg?label=conda&logo=conda-forge
   :target: https://anaconda.org/conda-forge/dvc
   :alt: Conda-forge

.. |PyPI| image:: https://img.shields.io/pypi/v/dvc.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/dvc
   :alt: PyPI

.. |Packages| image:: https://img.shields.io/github/v/release/iterative/dvc?label=deb|pkg|rpm|exe&logo=GitHub
   :target: https://github.com/iterative/dvc/releases/latest
   :alt: deb|pkg|rpm|exe

.. |DOI| image:: https://img.shields.io/badge/DOI-10.5281/zenodo.3677553-blue.svg
   :target: https://doi.org/10.5281/zenodo.3677553
   :alt: DOI

.. |Flowchart| image:: https://dvc.org/img/flow.gif
   :target: https://dvc.org/img/flow.gif
   :alt: how_dvc_works

.. |Contribs| image:: https://contrib.rocks/image?repo=iterative/dvc
   :target: https://github.com/iterative/dvc/graphs/contributors
   :alt: Contributors

.. |VS Code| image:: https://vsmarketplacebadge.apphb.com/version/Iterative.dvc.svg
   :target: https://marketplace.visualstudio.com/items?itemName=Iterative.dvc
   :alt: VS Code Extension
