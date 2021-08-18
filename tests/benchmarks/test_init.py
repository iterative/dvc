from functools import partial

import pytest

from tests.shell import Command


@pytest.mark.parametrize("git", [True, False], ids=["scm", "no_scm"])
def test_init(make_tmp_dir, dvc_cmd: Command, benchmark, git):
    tmp_dir = make_tmp_dir("init", scm=git)
    init_cmd = dvc_cmd.args("init")
    args = [] if git else ["--no-scm"]

    with tmp_dir.chdir():
        remove_dvc_dir = partial((tmp_dir / ".dvc").rmtree, ignore_errors=True)
        result = benchmark.pedantic(
            init_cmd, args=args, setup=remove_dvc_dir, rounds=5
        )

    assert result.returncode == 0
    assert "Initialized DVC repository." in result.stdout
