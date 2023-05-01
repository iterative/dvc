DVC pytest plugin

dvc.testing.benchmarks
======================

Benchmark test definitions are now part of ``dvc.testing``.
For CLI usage and `bench.dvc.org <https://bench.dvc.org>`_ details see `dvc-bench <https://github.com/iterative/dvc-bench>`_.

``dvc.testing.benchmarks`` structure:

* cli: should be able to run these with any dvc (rpm, deb, pypi, snap, etc) (could be used in dvc-test repo too)

  * commands: granular tests for individual commands. These should have a cached setup, so that we could use them during rapid development instead of our hand-written scripts. Every test could be run in a separate machine.
  * stories: multistage start-to-end benchmarks, useful for testing workflows (e.g. in documentation, see test_sharing inspired by `Storing and sharing <https://dvc.org/doc/start/data-management/data-versioning#storing-and-sharing>`_. Every full story could be run in a separate machine.

* api: for python api only.

  * methods: granular tests for individual methods (e.g. ``api.open/read``). Same reasoning as in ``cli.commands``
  * stories: same as ``cli.stories`` but for our api. E.g. imagine using our api with pandas or smth like that.
