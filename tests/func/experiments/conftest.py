from textwrap import dedent

import pytest

from tests.func.test_repro_multistage import COPY_SCRIPT
from tests.unit.repo.experiments.conftest import test_queue  # noqa

DEFAULT_ITERATIONS = 2
CHECKPOINT_SCRIPT_FORMAT = dedent(
    """\
    import os
    import sys
    import shutil

    from dvc.api import make_checkpoint

    checkpoint_file = {}
    checkpoint_iterations = int({})
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as fobj:
            try:
                value = int(fobj.read())
            except ValueError:
                value = 0
    else:
        with open(checkpoint_file, "w"):
            pass
        value = 0

    shutil.copyfile({}, {})

    if os.getenv("DVC_CHECKPOINT"):
        for _ in range(checkpoint_iterations):
            value += 1
            with open(checkpoint_file, "w") as fobj:
                fobj.write(str(value))
            make_checkpoint()
"""
)
CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]", "sys.argv[2]", "sys.argv[3]", "sys.argv[4]"
)


@pytest.fixture
def exp_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python copy.py params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        name="copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")
    return stage


@pytest.fixture
def checkpoint_stage(tmp_dir, scm, dvc, mocker):
    mocker.patch("dvc.stage.run.Monitor.AWAIT", 0.01)

    tmp_dir.gen("checkpoint.py", CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd=(
            f"python checkpoint.py foo {DEFAULT_ITERATIONS} "
            "params.yaml metrics.yaml"
        ),
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        checkpoints=["foo"],
        deps=["checkpoint.py"],
        no_exec=True,
        name="checkpoint-file",
    )
    scm.add(["dvc.yaml", "checkpoint.py", "params.yaml", ".gitignore"])
    scm.commit("init")
    stage.iterations = DEFAULT_ITERATIONS
    return stage


@pytest.fixture
def failed_exp_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("copy.py", COPY_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.stage.add(
        cmd="python -c 'import sys; sys.exit(1)'",
        metrics_no_cache=["failed-metrics.yaml"],
        params=["foo"],
        name="failed-copy-file",
        deps=["copy.py"],
    )
    scm.add(
        [
            "dvc.yaml",
            "dvc.lock",
            "copy.py",
            "params.yaml",
            "failed-metrics.yaml",
            ".gitignore",
        ]
    )
    scm.commit("init")
    return stage


@pytest.fixture
def http_auth_patch(mocker):
    from dulwich.client import HTTPUnauthorized

    url = "https://0.0.0.0"
    client = mocker.MagicMock()
    client.get_refs.side_effect = HTTPUnauthorized("", url)
    client.send_pack.side_effect = HTTPUnauthorized("", url)

    patch = mocker.patch("dulwich.client.get_transport_and_path")
    patch.return_value = (client, url)
    return url


@pytest.fixture(params=[True, False])
def workspace(request, test_queue) -> bool:  # noqa
    return request.param
