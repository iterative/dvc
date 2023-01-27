import pytest
from funcy import first

from dvc.repo.experiments.show import (
    ExpStatus,
    get_branch_names,
    move_properties_to_head,
    update_names,
)


def test_get_show_branch(tmp_dir, scm, dvc, exp_stage):
    new_branch = "new"

    baseline = scm.get_rev()
    base_branch = scm.active_branch()
    results = dvc.experiments.run(exp_stage.addressing, params=["foo=2"])
    exp_a = first(results)
    dvc.experiments.branch(exp_a, new_branch)

    scm.checkout(new_branch, force=True)

    result = {
        "workspace": {"baseline": {"data": {}}},
        exp_a: {"baseline": {"data": {}}},
        baseline: {"baseline": {"data": {}}, exp_a: {"data": {}}},
    }

    branch_names = get_branch_names(scm, ["workspace", exp_a, baseline])
    assert branch_names == {exp_a: new_branch, baseline: base_branch}

    update_names(dvc, branch_names, result)
    assert result[exp_a]["baseline"]["data"] == {"name": new_branch}
    assert result[baseline]["baseline"]["data"] == {"name": base_branch}
    assert result[baseline][exp_a]["data"] == {"name": new_branch}


@pytest.mark.parametrize(
    "res_in,res_out",
    [
        (
            {
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "3": {
                    "data": {
                        "checkpoint_tip": "3",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "4": {
                    "data": {
                        "checkpoint_tip": "3",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "b",
                    }
                },
            },
            {
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "3": {
                    "data": {
                        "checkpoint_tip": "3",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "b",
                    }
                },
                "4": {
                    "data": {
                        "checkpoint_tip": "3",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
            },
        ),
        (
            {
                "0": {"error": {"msg": "something"}},
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "3": {"data": {"name": "b"}},
            },
            {
                "0": {"error": {"msg": "something"}},
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "3": {"data": {"name": "b"}},
            },
        ),
        (
            {
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "3": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "b",
                    }
                },
                "4": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                    }
                },
            },
            {
                "1": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": "local",
                        "status": ExpStatus.Running.name,
                        "name": "a",
                    }
                },
                "2": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
                "3": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                        "name": "b",
                    }
                },
                "4": {
                    "data": {
                        "checkpoint_tip": "1",
                        "executor": None,
                        "status": ExpStatus.Success.name,
                    }
                },
            },
        ),
    ],
)
def test_move_properties_to_head(res_in, res_out):
    move_properties_to_head({"baseline": res_in})
    assert res_in == res_out
