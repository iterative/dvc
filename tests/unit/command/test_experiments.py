import csv
import textwrap
from datetime import datetime

import pytest

from dvc.cli import parse_args
from dvc.command.experiments.apply import CmdExperimentsApply
from dvc.command.experiments.branch import CmdExperimentsBranch
from dvc.command.experiments.diff import CmdExperimentsDiff
from dvc.command.experiments.gc import CmdExperimentsGC
from dvc.command.experiments.init import CmdExperimentsInit
from dvc.command.experiments.ls import CmdExperimentsList
from dvc.command.experiments.pull import CmdExperimentsPull
from dvc.command.experiments.push import CmdExperimentsPush
from dvc.command.experiments.remove import CmdExperimentsRemove
from dvc.command.experiments.run import CmdExperimentsRun
from dvc.command.experiments.show import CmdExperimentsShow, show_experiments
from dvc.exceptions import DvcParserError, InvalidArgumentError
from dvc.repo import Repo
from dvc.stage import PipelineStage
from tests.utils import ANY
from tests.utils.asserts import called_once_with_subset

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


def test_experiments_diff_revs(mocker, capsys):
    mocker.patch(
        "dvc.repo.experiments.diff.diff",
        return_value={
            "params": {
                "params.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}
            },
            "metrics": {
                "metrics.yaml": {"foo": {"diff": 1, "old": 1, "new": 2}}
            },
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


def test_show_experiments_csv(capsys):
    all_experiments = {
        "workspace": {
            "baseline": {
                "data": {
                    "timestamp": None,
                    "params": {
                        "params.yaml": {
                            "data": {
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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
                                "featurize": {
                                    "max_features": 3000,
                                    "ngrams": 1,
                                },
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

    show_experiments(
        all_experiments, precision=None, fill_value="", iso=True, csv=True
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


def test_show_experiments_md(capsys):
    all_experiments = {
        "workspace": {
            "baseline": {
                "data": {
                    "timestamp": None,
                    "params": {"params.yaml": {"data": {"foo": 1}}},
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {
                        "scores.json": {"data": {"bar": 0.9544670443829399}}
                    },
                }
            }
        },
    }
    show_experiments(
        all_experiments, precision=None, fill_value="", iso=True, markdown=True
    )
    cap = capsys.readouterr()

    assert cap.out == textwrap.dedent(
        """\
        | Experiment   | Created   | bar                | foo   |
        |--------------|-----------|--------------------|-------|
        | workspace    |           | 0.9544670443829399 | 1     |\n
    """
    )


@pytest.mark.parametrize("sort_order", ["asc", "desc"])
def test_show_experiments_sort_by(capsys, sort_order):
    sort_experiments = {
        "workspace": {
            "baseline": {
                "data": {
                    "timestamp": None,
                    "params": {
                        "params.yaml": {
                            "data": {
                                "foo": 1,
                            }
                        }
                    },
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {},
                }
            }
        },
        "233b132676792d89e848e5c9c12e408d7efde78a": {
            "baseline": {
                "data": {
                    "timestamp": datetime(2021, 8, 2, 16, 48, 14),
                    "params": {
                        "params.yaml": {
                            "data": {
                                "foo": 0,
                            }
                        }
                    },
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {},
                    "name": "master",
                }
            },
            "fad0a94": {
                "data": {
                    "timestamp": datetime(2021, 8, 31, 14, 56, 55),
                    "params": {
                        "params.yaml": {
                            "data": {
                                "foo": 1,
                            }
                        }
                    },
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {},
                    "name": "exp-89140",
                }
            },
            "60fcda8": {
                "data": {
                    "timestamp": datetime(2021, 8, 31, 14, 56, 55),
                    "params": {
                        "params.yaml": {
                            "data": {
                                "foo": 2,
                            }
                        }
                    },
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {},
                    "name": "exp-43537",
                }
            },
            "a7e9aaf": {
                "data": {
                    "timestamp": datetime(2021, 8, 31, 14, 56, 55),
                    "params": {
                        "params.yaml": {
                            "data": {
                                "foo": 0,
                            }
                        }
                    },
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {},
                    "name": "exp-4f89e",
                }
            },
        },
    }

    show_experiments(
        sort_experiments,
        precision=None,
        fill_value="",
        iso=True,
        csv=True,
        sort_by="foo",
        sort_order=sort_order,
    )

    cap = capsys.readouterr()
    rows = list(csv.reader(cap.out.strip().split("\n")))
    # [3:] To skip header, workspace and baseline(master)
    # which are not affected by order
    params = tuple([int(row[-1]) for row in rows[3:]])

    if sort_order == "asc":
        assert params == (0, 1, 2)
    else:
        assert params == (2, 1, 0)


@pytest.mark.parametrize("extra_args", [(), ("--run",)])
def test_experiments_init(dvc, scm, mocker, capsys, extra_args):
    m = mocker.patch(
        "dvc.repo.experiments.init.init",
        return_value=PipelineStage(dvc, path="dvc.yaml", name="stage1"),
    )
    runner = mocker.patch("dvc.repo.experiments.run.run", return_value=0)
    cli_args = parse_args(["exp", "init", *extra_args, "cmd"])
    cmd = cli_args.func(cli_args)

    assert isinstance(cmd, CmdExperimentsInit)
    assert cmd.run() == 0
    m.assert_called_once_with(
        ANY(Repo),
        name="train",
        type="default",
        defaults={
            "code": "src",
            "models": "models",
            "data": "data",
            "metrics": "metrics.json",
            "params": "params.yaml",
            "plots": "plots",
            "live": "dvclive",
        },
        overrides={"cmd": "cmd"},
        interactive=False,
        force=False,
    )
    expected = "Created train stage in dvc.yaml."
    if not extra_args:
        expected += (
            ' To run, use "dvc exp run".\n' "See https://s.dvc.org/g/exp/run."
        )
    assert capsys.readouterr() == (expected + "\n", "")
    if extra_args:
        # `parse_args` creates a new `Repo` object
        runner.assert_called_once_with(ANY(Repo), targets=["stage1"])


def test_experiments_init_config(dvc, scm, mocker):
    with dvc.config.edit() as conf:
        conf["exp"] = {"code": "new_src", "models": "new_models"}

    m = mocker.patch("dvc.repo.experiments.init.init")
    cli_args = parse_args(["exp", "init", "cmd"])
    cmd = cli_args.func(cli_args)

    assert isinstance(cmd, CmdExperimentsInit)
    assert cmd.run() == 0

    m.assert_called_once_with(
        ANY(Repo),
        name="train",
        type="default",
        defaults={
            "code": "new_src",
            "models": "new_models",
            "data": "data",
            "metrics": "metrics.json",
            "params": "params.yaml",
            "plots": "plots",
            "live": "dvclive",
        },
        overrides={"cmd": "cmd"},
        interactive=False,
        force=False,
    )


def test_experiments_init_explicit(dvc, mocker):
    m = mocker.patch("dvc.repo.experiments.init.init")
    cli_args = parse_args(["exp", "init", "--explicit", "cmd"])
    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0
    m.assert_called_once_with(
        ANY(Repo),
        name="train",
        type="default",
        defaults={},
        overrides={"cmd": "cmd"},
        interactive=False,
        force=False,
    )


def test_experiments_init_cmd_required_for_non_interactive_mode(dvc):
    cli_args = parse_args(["exp", "init"])
    cmd = cli_args.func(cli_args)
    assert isinstance(cmd, CmdExperimentsInit)

    with pytest.raises(InvalidArgumentError) as exc:
        cmd.run()
    assert str(exc.value) == "command is not specified"


def test_experiments_init_cmd_not_required_for_interactive_mode(dvc, mocker):
    cli_args = parse_args(["exp", "init", "--interactive"])
    cmd = cli_args.func(cli_args)
    assert isinstance(cmd, CmdExperimentsInit)

    m = mocker.patch("dvc.repo.experiments.init.init")
    assert cmd.run() == 0
    assert called_once_with_subset(m, ANY(Repo), interactive=True)


@pytest.mark.parametrize(
    "extra_args, expected_kw",
    [
        (["--type", "default"], {"type": "default", "name": "train"}),
        (["--type", "dl"], {"type": "dl", "name": "train"}),
        (["--force"], {"force": True, "name": "train"}),
        (
            ["--name", "name", "--type", "dl"],
            {"name": "name", "type": "dl"},
        ),
        (
            [
                "--plots",
                "p",
                "--models",
                "m",
                "--code",
                "c",
                "--metrics",
                "m.json",
                "--params",
                "p.yaml",
                "--data",
                "d",
                "--live",
                "live",
            ],
            {
                "name": "train",
                "overrides": {
                    "plots": "p",
                    "models": "m",
                    "code": "c",
                    "metrics": "m.json",
                    "params": "p.yaml",
                    "data": "d",
                    "live": "live",
                    "cmd": "cmd",
                },
            },
        ),
    ],
)
def test_experiments_init_extra_args(extra_args, expected_kw, mocker):
    cli_args = parse_args(["exp", "init", *extra_args, "cmd"])
    cmd = cli_args.func(cli_args)
    assert isinstance(cmd, CmdExperimentsInit)

    m = mocker.patch("dvc.repo.experiments.init.init")
    assert cmd.run() == 0
    assert called_once_with_subset(m, ANY(Repo), **expected_kw)


def test_experiments_init_type_invalid_choice():
    with pytest.raises(DvcParserError):
        parse_args(["exp", "init", "--type=invalid", "cmd"])


def test_show_experiments_html(tmp_dir, mocker):
    all_experiments = {
        "workspace": {
            "baseline": {
                "data": {
                    "timestamp": None,
                    "params": {"params.yaml": {"data": {"foo": 1}}},
                    "queued": False,
                    "running": False,
                    "executor": None,
                    "metrics": {
                        "scores.json": {"data": {"bar": 0.9544670443829399}}
                    },
                }
            }
        },
    }
    experiments_table = mocker.patch(
        "dvc.command.experiments.show.experiments_table"
    )
    td = experiments_table.return_value

    show_experiments(all_experiments, html=True)

    td.dropna.assert_called_with("rows", how="all")

    render_kwargs = td.render.call_args[1]

    for arg in ["html", "output_path", "color_by"]:
        assert arg in render_kwargs
    assert render_kwargs["output_path"] == tmp_dir / "dvc_plots"
    assert render_kwargs["color_by"] == "Experiment"
