from funcy import first
from scmrepo.git import Git

from dvc.repo.experiments.show import get_names


def test_get_show_branch(tmp_dir, scm: "Git", dvc, exp_stage):
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

    get_names(dvc, result)
    assert result[exp_a]["baseline"]["data"] == {"name": new_branch}
    assert result[baseline]["baseline"]["data"] == {"name": base_branch}
    assert result[baseline][exp_a]["data"] == {"name": new_branch}
