from dvc.api.scm import all_branches, all_commits, all_tags


def test_all_branches(tmp_dir, scm, dvc):
    assert all_branches() == ["master"]

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("branch", "branch", "commit")

    assert all_branches() == ["branch", "master"]


def test_all_commits(tmp_dir, scm, dvc):
    first = scm.get_rev()
    assert all_commits() == [first]

    tmp_dir.scm_gen("foo", "foo", "commit")
    second = scm.get_rev()

    assert set(all_commits()) == {first, second}


def test_all_tags(tmp_dir, scm, dvc):
    scm.tag("v1")
    assert all_tags() == ["v1"]

    tmp_dir.scm_gen("foo", "foo", "commit")
    scm.tag("v2")

    assert set(all_tags()) == {"v1", "v2"}
