import pytest

from dvc.cli import parse_args
from dvc.commands.experiments.apply import CmdExperimentsApply
from dvc.commands.experiments.branch import CmdExperimentsBranch
from dvc.commands.experiments.clean import CmdExperimentsClean
from dvc.commands.experiments.diff import CmdExperimentsDiff
from dvc.commands.experiments.ls import CmdExperimentsList
from dvc.commands.experiments.pull import CmdExperimentsPull
from dvc.commands.experiments.push import CmdExperimentsPush
from dvc.commands.experiments.remove import CmdExperimentsRemove
from dvc.commands.experiments.run import CmdExperimentsRun
from dvc.commands.experiments.save import CmdExperimentsSave
from dvc.commands.experiments.show import CmdExperimentsShow
from dvc.exceptions import InvalidArgumentError

from .test_repro import common_arguments as repro_arguments


def test_experiments_apply(dvc, scm, mocker):
    cli_args = parse_args(["experiments", "apply", "exp_rev"])
    assert cli_args.func == CmdExperimentsApply

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.apply.apply", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(cmd.repo, "exp_rev")


def test_experiments_diff(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--all",
            "--param-deps",
            "--json",
            "--md",
            "--precision",
            "10",
        ]
    )
    assert cli_args.func == CmdExperimentsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.diff.diff", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo, a_rev="HEAD~10", b_rev="HEAD~1", all=True, param_deps=True
    )


def test_experiments_diff_revs(mocker, capsys, dvc, scm):
    mocker.patch(
        "dvc.repo.experiments.diff.diff",
        return_value={
            "params": {"params.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}},
            "metrics": {"metrics.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}},
        },
    )

    cli_args = parse_args(["exp", "diff", "exp_a", "exp_b"])
    cmd = cli_args.func(cli_args)

    capsys.readouterr()
    assert cmd.run() == 0
    cap = capsys.readouterr()
    assert "exp_a" in cap.out
    assert "exp_b" in cap.out


def test_experiments_show(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "show",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--hide-queued",
            "--hide-failed",
            "--sha",
            "--param-deps",
            "-n",
            "1",
            "--rev",
            "foo",
            "--force",
        ]
    )
    assert cli_args.func == CmdExperimentsShow

    cmd = cli_args.func(cli_args)

    m = mocker.patch("dvc.repo.experiments.show.show", return_value={})
    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        hide_queued=True,
        hide_failed=True,
        num=1,
        revs=["foo"],
        sha_only=True,
        param_deps=True,
        fetch_running=True,
        force=True,
    )


def test_experiments_run(dvc, scm, mocker):
    default_arguments = {
        "params": [],
        "name": None,
        "queue": False,
        "run_all": False,
        "jobs": 1,
        "tmp_dir": False,
        "machine": None,
        "copy_paths": [],
        "message": None,
    }
    default_arguments.update(repro_arguments)

    cmd = CmdExperimentsRun(parse_args(["exp", "run"]))
    mocker.patch.object(cmd.repo, "reproduce")
    mocker.patch.object(cmd.repo.experiments, "run")
    cmd.run()
    cmd.repo.experiments.run.assert_called_with(**default_arguments)


def test_experiments_branch(dvc, scm, mocker):
    m = mocker.patch("dvc.repo.experiments.branch.branch", return_value={})

    cli_args = parse_args(["experiments", "branch", "expname"])
    assert cli_args.func == CmdExperimentsBranch

    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0

    m.assert_called_with(cmd.repo, "expname", None)

    cli_args = parse_args(["experiments", "branch", "expname", "branchname"])
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0

    m.assert_called_with(cmd.repo, "expname", "branchname")


def test_experiments_list(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "list",
            "origin",
            "--all-commits",
            "-n",
            "-1",
            "--rev",
            "foo",
            "--name-only",
        ]
    )
    assert cli_args.func == CmdExperimentsList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.ls.ls", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        git_remote="origin",
        rev=["foo"],
        all_commits=True,
        num=-1,
    )


@pytest.mark.parametrize(
    "args,expected",
    [
        ([], "main:\n\tsha-a [exp-a]\n"),
        (["--name-only"], "exp-a\n"),
        (["--sha-only"], "sha-a\n"),
    ],
)
def test_experiments_list_format(mocker, capsys, args, expected, dvc, scm):
    mocker.patch(
        "dvc.repo.experiments.ls.ls",
        return_value={
            "refs/heads/main": [
                ("exp-a", "sha-a"),
            ]
        },
    )
    raw_args = ["experiments", "list", *args]
    cli_args = parse_args(raw_args)

    cmd = cli_args.func(cli_args)

    capsys.readouterr()
    assert cmd.run() == 0
    cap = capsys.readouterr()
    assert cap.out == expected


def test_experiments_list_remote(mocker, capsys, dvc, scm):
    mocker.patch(
        "dvc.repo.experiments.ls.ls",
        return_value={
            "main": [
                ("exp-a", None),
            ]
        },
    )
    cli_args = parse_args(["experiments", "list", "git_remote"])

    cmd = cli_args.func(cli_args)

    capsys.readouterr()
    assert cmd.run() == 0
    cap = capsys.readouterr()
    assert cap.out == "main:\n\texp-a\n"

    cli_args = parse_args(["experiments", "list", "git_remote", "--sha-only"])

    cmd = cli_args.func(cli_args)

    capsys.readouterr()

    with pytest.raises(InvalidArgumentError):
        cmd.run()


def test_experiments_push(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "push",
            "origin",
            "experiment1",
            "experiment2",
            "--all-commits",
            "-n",
            "2",
            "--rev",
            "foo",
            "--force",
            "--no-cache",
            "--remote",
            "my-remote",
            "--jobs",
            "1",
            "--run-cache",
        ]
    )
    assert cli_args.func == CmdExperimentsPush

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.push.push", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        "origin",
        ["experiment1", "experiment2"],
        rev=["foo"],
        all_commits=True,
        num=2,
        force=True,
        push_cache=False,
        dvc_remote="my-remote",
        jobs=1,
        run_cache=True,
    )

    cli_args = parse_args(
        [
            "experiments",
            "push",
            "origin",
        ]
    )
    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0


def test_experiments_pull(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "pull",
            "origin",
            "experiment",
            "--all-commits",
            "--rev",
            "foo",
            "--force",
            "--no-cache",
            "--remote",
            "my-remote",
            "--jobs",
            "1",
            "--run-cache",
        ]
    )
    assert cli_args.func == CmdExperimentsPull

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.pull.pull", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        "origin",
        ["experiment"],
        rev=["foo"],
        all_commits=True,
        num=1,
        force=True,
        pull_cache=False,
        dvc_remote="my-remote",
        jobs=1,
        run_cache=True,
    )

    cli_args = parse_args(
        [
            "experiments",
            "pull",
            "origin",
        ]
    )
    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0


def test_experiments_remove_flag(dvc, scm, mocker, capsys, caplog):
    cli_args = parse_args(
        [
            "experiments",
            "remove",
            "--all-commits",
            "--rev",
            "foo",
            "--num",
            "2",
            "--git-remote",
            "myremote",
        ]
    )
    assert cli_args.func == CmdExperimentsRemove
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.remove.remove", return_value={})
    assert cmd.run() == 0
    m.assert_called_once_with(
        cmd.repo,
        exp_names=[],
        all_commits=True,
        rev=["foo"],
        num=2,
        queue=False,
        git_remote="myremote",
    )


def test_experiments_remove_special(dvc, scm, mocker, capsys, caplog):
    cli_args = parse_args(
        [
            "experiments",
            "remove",
            "--git-remote",
            "myremote",
            "exp-123",
            "exp-234",
        ]
    )
    assert cli_args.func == CmdExperimentsRemove
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.remove.remove", return_value={})
    assert cmd.run() == 0
    m.assert_called_once_with(
        cmd.repo,
        exp_names=["exp-123", "exp-234"],
        all_commits=False,
        rev=None,
        num=1,
        queue=False,
        git_remote="myremote",
    )


def test_experiments_remove_invalid(dvc, scm, mocker, capsys, caplog):
    cmd = CmdExperimentsRemove(
        parse_args(["exp", "remove", "--all-commits", "exp-1", "exp-2"])
    )
    with pytest.raises(InvalidArgumentError):
        cmd.run()
    cmd = CmdExperimentsRemove(parse_args(["exp", "remove"]))
    with pytest.raises(InvalidArgumentError) as excinfo:
        cmd.run()
    assert (
        str(excinfo.value) == "Either provide an `experiment` argument"
        ", or use the `--rev` or `--all-commits` or `--queue` flag."
    )


def test_experiments_save(dvc, scm, mocker):
    cli_args = parse_args(["exp", "save", "--name", "exp-name", "--force"])
    assert cli_args.func == CmdExperimentsSave

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.save.save", return_value="acabb")

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo, name="exp-name", force=True, include_untracked=[], message=None
    )


def test_experiments_clean(dvc, scm, mocker):
    cli_args = parse_args(["experiments", "clean"])
    assert cli_args.func == CmdExperimentsClean

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.clean.clean", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(cmd.repo)
