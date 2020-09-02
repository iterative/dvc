import os
from unittest.mock import call

import pytest

from dvc.external_repo import external_repo
from tests.unit.tree.test_repo import make_subrepo


def test_hook_is_called(tmp_dir, erepo_dir, mocker):
    subrepo_paths = [
        "subrepo1",
        "subrepo2",
        "dir/subrepo3",
        "dir/subrepo4",
        "subrepo5",
        "subrepo5/subrepo6",
    ]
    subrepos = [erepo_dir / path for path in subrepo_paths]
    for repo in subrepos:
        make_subrepo(repo, erepo_dir.scm)

    for repo in subrepos + [erepo_dir]:
        with repo.chdir():
            repo.scm_gen("foo", "foo", commit=f"git add {repo}/foo")
            repo.dvc_gen("bar", "bar", commit=f"dvc add {repo}/bar")

    with external_repo(str(erepo_dir)) as repo:
        spy = mocker.patch.object(repo, "make_repo", wraps=repo.make_repo)

        list(repo.repo_tree.walk(repo.root_dir))  # drain
        assert spy.call_count == len(subrepos)

        paths = [os.path.join(repo.root_dir, path) for path in subrepo_paths]
        spy.assert_has_calls([call(path) for path in paths], any_order=True)


@pytest.mark.parametrize("top_level_dvc", [False, True])
def test_subrepo_is_constructed_properly(
    tmp_dir, dvc, scm, mocker, make_tmp_dir, top_level_dvc
):
    subrepo = tmp_dir / "subrepo"
    make_subrepo(subrepo, scm)
    local_cache = subrepo.dvc.cache.local.cache_dir

    tmp_dir.scm_gen("bar", "bar", commit="add bar")
    subrepo.dvc_gen("foo", "foo", commit="add foo")

    cache_dir = make_tmp_dir("temp-cache")
    with external_repo(
        str(tmp_dir), cache_dir=str(cache_dir), cache_types=["symlink"]
    ) as repo:
        spy = mocker.spy(repo, "make_repo")

        list(repo.repo_tree.walk(repo.root_dir))  # drain
        assert spy.call_count == 1
        subrepo = spy.return_value

        assert repo.url == str(tmp_dir)
        assert repo.cache_dir == str(cache_dir)
        assert repo.cache.local.cache_dir == str(cache_dir)
        assert subrepo.cache.local.cache_dir == str(cache_dir)

        assert repo.cache_types == ["symlink"]
        assert repo.cache.local.cache_types == ["symlink"]
        assert subrepo.cache.local.cache_types == ["symlink"]

        assert (
            subrepo.config["remote"]["auto-generated-upstream"]["url"]
            == local_cache
        )
        if top_level_dvc:
            main_cache = tmp_dir.dvc.cache.local.cache_dir
            assert repo.config["remote"]["auto-generated-upstream"][
                "url"
            ] == str(main_cache)
