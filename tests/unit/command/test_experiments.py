from datetime import datetime

import pytest

from dvc.cli import parse_args
from dvc.command.experiments import (
    CmdExperimentsApply,
    CmdExperimentsBranch,
    CmdExperimentsDiff,
    CmdExperimentsGC,
    CmdExperimentsList,
    CmdExperimentsPull,
    CmdExperimentsPush,
    CmdExperimentsRemove,
    CmdExperimentsRun,
    CmdExperimentsShow,
    show_experiments,
)
from dvc.exceptions import InvalidArgumentError

from .test_repro import default_arguments as repro_arguments


def test_experiments_apply(dvc, scm, mocker):
    cli_args = parse_args(["experiments", "apply", "--no-force", "exp_rev"])
    assert cli_args.func == CmdExperimentsApply

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.apply.apply", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(cmd.repo, "exp_rev", force=False)


def test_experiments_diff(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--all",
            "--param-deps",
            "--show-json",
            "--show-md",
            "--old",
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


def test_experiments_show(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "show",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--sha",
            "--param-deps",
            "-n",
            "1",
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
        sha_only=True,
        num=1,
        param_deps=True,
    )


def test_experiments_run(dvc, scm, mocker):
    default_arguments = {
        "params": [],
        "name": None,
        "queue": False,
        "run_all": False,
        "jobs": 1,
        "tmp_dir": False,
        "checkpoint_resume": None,
        "reset": False,
    }
    default_arguments.update(repro_arguments)

    cmd = CmdExperimentsRun(parse_args(["exp", "run"]))
    mocker.patch.object(cmd.repo, "reproduce")
    mocker.patch.object(cmd.repo.experiments, "run")
    cmd.run()
    # pylint: disable=no-member
    cmd.repo.experiments.run.assert_called_with(**default_arguments)


def test_experiments_gc(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "exp",
            "gc",
            "--workspace",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--queued",
            "--force",
        ]
    )
    assert cli_args.func == CmdExperimentsGC

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.gc.gc", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        workspace=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        queued=True,
    )

    cli_args = parse_args(["exp", "gc"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()


def test_experiments_branch(dvc, scm, mocker):
    cli_args = parse_args(["experiments", "branch", "expname", "branchname"])
    assert cli_args.func == CmdExperimentsBranch

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.branch.branch", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(cmd.repo, "expname", "branchname")


def test_experiments_list(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "list",
            "origin",
            "--rev",
            "foo",
            "--all",
            "--names-only",
        ]
    )
    assert cli_args.func == CmdExperimentsList

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.ls.ls", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo, git_remote="origin", rev="foo", all_=True
    )


def test_experiments_push(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "push",
            "origin",
            "experiment",
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
        "experiment",
        force=True,
        push_cache=False,
        dvc_remote="my-remote",
        jobs=1,
        run_cache=True,
    )


def test_experiments_pull(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "pull",
            "origin",
            "experiment",
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
        "experiment",
        force=True,
        pull_cache=False,
        dvc_remote="my-remote",
        jobs=1,
        run_cache=True,
    )


@pytest.mark.parametrize(
    "queue,clear_all,remote",
    [(True, False, None), (False, True, None), (False, False, True)],
)
def test_experiments_remove(dvc, scm, mocker, queue, clear_all, remote):
    if queue:
        args = ["--queue"]
    if clear_all:
        args = ["--all"]
    if remote:
        args = ["--git-remote", "myremote", "exp-123", "exp-234"]
    cli_args = parse_args(["experiments", "remove"] + args)
    assert cli_args.func == CmdExperimentsRemove

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.remove.remove", return_value={})

    assert cmd.run() == 0
    m.assert_called_once_with(
        cmd.repo,
        exp_names=["exp-123", "exp-234"] if remote else [],
        queue=queue,
        clear_all=clear_all,
        remote="myremote" if remote else None,
    )


all_experiments = {
    "workspace": {
        "baseline": {
            "data": {
                "timestamp": None,
                "params": {
                    "params.yaml": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "parent": 20170428,
                            "train": {
                                "n_est": 100,
                                "min_split": 36,
                            },
                        }
                    }
                },
                "queued": False,
                "running": False,
                "executor": None,
                "metrics": {
                    "scores.json": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "avg_prec": 0.5843640011189556,
                            "roc_auc": 0.9544670443829399,
                        }
                    }
                },
            }
        }
    },
    "b05eecc666734e899f79af228ff49a7ae5a18cc0": {
        "baseline": {
            "data": {
                "timestamp": datetime(2021, 8, 2, 16, 48, 14),
                "params": {
                    "params.yaml": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "parent": 20170428,
                            "train": {
                                "n_est": 100,
                                "min_split": 2,
                            },
                        }
                    }
                },
                "queued": False,
                "running": False,
                "executor": None,
                "metrics": {
                    "scores.json": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "avg_prec": 0.5325162867864254,
                            "roc_auc": 0.9106964878520005,
                        }
                    }
                },
                "name": "master",
            }
        },
        "ae99936461d6c3092934160f8beafe66a294f98d": {
            "data": {
                "timestamp": datetime(2021, 8, 31, 14, 56, 55),
                "params": {
                    "params.yaml": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "parent": 20170428,
                            "train": {
                                "n_est": 100,
                                "min_split": 36,
                            },
                        }
                    }
                },
                "queued": True,
                "running": True,
                "executor": None,
                "metrics": {
                    "scores.json": {
                        "data": {
                            "featurize": {"max_features": 3000, "ngrams": 1},
                            "avg_prec": 0.5843640011189556,
                            "roc_auc": 0.9544670443829399,
                        }
                    }
                },
                "name": "exp-44136",
            }
        },
    },
}


def test_show_experiments(capsys):
    show_experiments(
        all_experiments, precision=None, fill_value="", iso=True, show_csv=True
    )
    cap = capsys.readouterr()
    assert (
        "Experiment,rev,typ,Created,parent,State,scores.json:"
        "featurize.max_features,scores.json:featurize.ngrams,"
        "avg_prec,roc_auc,params.yaml:featurize.max_features,"
        "params.yaml:featurize.ngrams,params.yaml:parent,"
        "train.n_est,train.min_split" in cap.out
    )
    assert (
        ",workspace,baseline,,,,3000,1,0.5843640011189556,0.9544670443829399,"
        "3000,1,20170428,100,36" in cap.out
    )
    assert (
        "master,b05eecc,baseline,2021-08-02T16:48:14,,,3000,1,"
        "0.5325162867864254,0.9106964878520005,3000,1,20170428,100,2"
        in cap.out
    )
    assert (
        "exp-44136,ae99936,branch_base,2021-08-31T14:56:55,,Running,"
        "3000,1,0.5843640011189556,0.9544670443829399,3000,1,20170428,100,36"
        in cap.out
    )
