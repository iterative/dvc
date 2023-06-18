import os

import pytest

from dvc.repo.open_repo import _external_repo as external_repo
from dvc.testing.tmp_dir import make_subrepo


def test_hook_is_called(tmp_dir, erepo_dir, mocker):
    subrepo_paths = [
        "subrepo1",
        "subrepo2",
        os.path.join("dir", "subrepo3"),
        os.path.join("dir", "subrepo4"),
        "subrepo5",
        os.path.join("subrepo5", "subrepo6"),
    ]
    subrepos = [erepo_dir / path for path in subrepo_paths]
    for repo in subrepos:
        make_subrepo(repo, erepo_dir.scm)

    for repo in [*subrepos, erepo_dir]:
        with repo.chdir():
            repo.scm_gen("foo", "foo", commit=f"git add {repo}/foo")
            repo.dvc_gen("bar", "bar", commit=f"dvc add {repo}/bar")

    with external_repo(str(erepo_dir), subrepos=True, uninitialized=True) as repo:
        spy = mocker.spy(repo.dvcfs.fs, "repo_factory")

        list(repo.dvcfs.walk("", ignore_subrepos=False))  # drain
        assert spy.call_count == len(subrepos)

        paths = ["/" + path.replace("\\", "/") for path in subrepo_paths]
        spy.assert_has_calls(
            [
                mocker.call(
                    path,
                    fs=repo.fs,
                    scm=repo.scm,
                    repo_factory=repo.dvcfs.fs.repo_factory,
                )
                for path in paths
            ],
            any_order=True,
        )


@pytest.mark.parametrize("root_is_dvc", [False, True])
def test_subrepo_is_constructed_properly(
    tmp_dir, scm, mocker, make_tmp_dir, root_is_dvc
):
    if root_is_dvc:
        make_subrepo(tmp_dir, scm)

    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    local_cache = subrepo.dvc.cache.local_cache_dir

    tmp_dir.scm_gen("bar", "bar", commit="add bar")
    subrepo.dvc_gen("foo", "foo", commit="add foo")

    cache_dir = make_tmp_dir("temp-cache")
    with external_repo(
        str(tmp_dir),
        subrepos=True,
        uninitialized=True,
        config={"cache": {"dir": str(cache_dir), "type": ["symlink"]}},
    ) as repo:
        spy = mocker.spy(repo.dvcfs.fs, "repo_factory")

        list(repo.dvcfs.walk("", ignore_subrepos=False))  # drain
        assert spy.call_count == 1
        subrepo = spy.spy_return

        assert repo.url == str(tmp_dir)
        assert repo.config["cache"]["dir"] == str(cache_dir)
        assert repo.cache.local.path == os.path.join(cache_dir, "files", "md5")
        assert subrepo.cache.local.path == os.path.join(cache_dir, "files", "md5")

        assert repo.config["cache"]["type"] == ["symlink"]
        assert repo.cache.local.cache_types == ["symlink"]
        assert subrepo.cache.local.cache_types == ["symlink"]

        assert subrepo.config["remote"]["auto-generated-upstream"]["url"] == local_cache
        if root_is_dvc:
            main_cache = tmp_dir.dvc.cache.local_cache_dir
            assert repo.config["remote"]["auto-generated-upstream"]["url"] == main_cache
