"""
Tests for modifications to an existing dataset.
"""
import glob
import os
import random
import shutil
import sys

import pytest

# pylint: disable=unused-argument,unexpected-keyword-arg


@pytest.mark.skipif(sys.version_info < (3, 10), reason="requires 3.10 glob.glob")
def test_partial_add(bench_dvc, tmp_dir, dvc, dataset, remote):
    random.seed(4231)
    # Move some files to create a partial dataset
    os.makedirs("partial-copy")
    for f in glob.glob("*", root_dir=dataset, recursive=True):  # type: ignore[call-arg]
        if random.random() > 0.5:  # noqa: S311 # nosec
            shutil.move(dataset / f, tmp_dir / "partial-copy" / f)

    # Add/push partial dataset
    bench_dvc("add", dataset)
    bench_dvc("push")

    # Add more files to the dataset
    shutil.copytree("partial-copy", dataset, dirs_exist_ok=True)

    # Benchmark operations for adding files to a dataset
    bench_dvc("add", dataset, name="partial-add")
    bench_dvc("push", name="partial-add")
    bench_dvc("gc", "-f", "-w", name="noop")
    bench_dvc("gc", "-f", "-w", "-c", name="cloud-noop")


@pytest.mark.skipif(sys.version_info < (3, 10), reason="requires 3.10 glob.glob")
def test_partial_remove(bench_dvc, tmp_dir, dvc, dataset, remote):
    random.seed(5232)
    # Add/push full dataset
    bench_dvc("add", dataset)
    bench_dvc("push")

    # Remove some files
    for f in glob.glob("*", root_dir=dataset, recursive=True):  # type: ignore[call-arg]
        if random.random() > 0.5:  # noqa: S311 # nosec
            if os.path.isfile(dataset / f):
                os.remove(dataset / f)
            elif os.path.isdir(dataset / f):
                shutil.rmtree(dataset / f)

    # Benchmark operations for removing files from dataset
    bench_dvc("add", dataset)
    bench_dvc("push")
    bench_dvc("gc", "-f", "-w")
    bench_dvc("gc", "-f", "-w", "-c", name="cloud")
