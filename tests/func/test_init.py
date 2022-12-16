import logging
import os

import pytest

from dvc.cli import main
from dvc.config import Config
from dvc.exceptions import InitError
from dvc.repo import Repo as DvcRepo


def test_api_init(scm):
    DvcRepo.init().close()
    assert os.path.isdir(DvcRepo.DVC_DIR)


def test_cli_init(scm):
    ret = main(["init"])
    assert ret == 0
    assert os.path.isdir(DvcRepo.DVC_DIR)


def test_double_init(scm):
    ret = main(["init"])
    assert ret == 0
    assert os.path.isdir(DvcRepo.DVC_DIR)

    ret = main(["init"])
    assert ret != 0
    assert os.path.isdir(DvcRepo.DVC_DIR)

    ret = main(["init", "--force"])
    assert ret == 0
    assert os.path.isdir(DvcRepo.DVC_DIR)


def test_init_no_scm_fail_api(tmp_dir):
    with pytest.raises(InitError):
        DvcRepo.init()


def test_init_no_scm_fail_cli(tmp_dir):
    ret = main(["init"])
    assert ret != 0


def test_init_no_scm_api(tmp_dir):
    repo = DvcRepo.init(no_scm=True)

    assert (tmp_dir / DvcRepo.DVC_DIR).is_dir()
    assert repo.config["core"]["no_scm"]


def test_init_no_scm_cli(tmp_dir):
    ret = main(["init", "--no-scm"])
    assert ret == 0

    dvc_path = tmp_dir / DvcRepo.DVC_DIR
    assert dvc_path.is_dir()
    assert Config(os.fspath(dvc_path))["core"]["no_scm"]


def test_init_quiet_should_not_display_welcome_screen(tmp_dir, scm, caplog):
    with caplog.at_level(logging.INFO, logger="dvc"):
        ret = main(["init", "--quiet"])

        assert ret == 0
        assert "" == caplog.text


def test_allow_init_dvc_subdir(tmp_dir, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})

    with monkeypatch.context() as m:
        m.chdir("subdir")
        assert main(["init", "--subdir"]) == 0

    repo = DvcRepo("subdir")
    assert repo.root_dir == os.fspath(tmp_dir / "subdir")
    assert repo.scm.root_dir == os.fspath(tmp_dir)


def test_subdir_init_no_option(tmp_dir, scm, monkeypatch, caplog):
    tmp_dir.gen({"subdir": {}})

    caplog.clear()
    with monkeypatch.context() as m:
        m.chdir("subdir")
        with caplog.at_level(logging.ERROR, logger="dvc"):
            assert main(["init"]) == 1

    assert (
        "{} is not tracked by any supported SCM tool (e.g. Git). "
        "Use `--no-scm` if you don't want to use any SCM or "
        "`--subdir` if initializing inside a subdirectory of a parent SCM "
        "repository.".format(os.fspath(tmp_dir / "subdir"))
    ) in caplog.text


def test_gen_dvcignore(tmp_dir):
    DvcRepo.init(no_scm=True)
    text = (
        "# Add patterns of files dvc should ignore, which could improve\n"
        "# the performance. Learn more at\n"
        "# https://dvc.org/doc/user-guide/dvcignore\n"
    )
    assert text == (tmp_dir / ".dvcignore").read_text()


def test_init_when_ignored_by_git(tmp_dir, scm, caplog):
    # https://github.com/iterative/dvc/issues/3738
    tmp_dir.gen({".gitignore": ".*"})
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert main(["init"]) == 1
    assert (
        "{dvc_dir} is ignored by your SCM tool. \n"
        "Make sure that it's tracked, "
        "for example, by adding '!.dvc' to .gitignore.".format(
            dvc_dir=tmp_dir / DvcRepo.DVC_DIR
        )
    ) in caplog.text
