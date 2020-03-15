import os

from funcy import raiser
import pytest

from dvc.repo import locked


def test_is_dvc_internal(dvc):
    assert dvc.is_dvc_internal(os.path.join("path", "to", ".dvc", "file"))
    assert not dvc.is_dvc_internal(os.path.join("path", "to-non-.dvc", "file"))


@pytest.mark.parametrize(
    "path",
    [
        os.path.join("dir", "subdir", "file"),
        os.path.join("dir", "subdir"),
        "dir",
    ],
)
def test_find_outs_by_path(tmp_dir, dvc, path):
    (stage,) = tmp_dir.dvc_gen(
        {"dir": {"subdir": {"file": "file"}, "other": "other"}}
    )

    outs = dvc.find_outs_by_path(path, strict=False)
    assert len(outs) == 1
    assert outs[0].path_info == stage.outs[0].path_info


@pytest.mark.parametrize(
    "path",
    [os.path.join("dir", "subdir", "file"), os.path.join("dir", "subdir")],
)
def test_used_cache(tmp_dir, dvc, path):
    from dvc.cache import NamedCache

    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}, "other": "other"}})
    expected = NamedCache.make(
        "local", "70922d6bf66eb073053a82f77d58c536.dir", "dir"
    )
    expected.add(
        "local",
        "8c7dd922ad47494fc02c388e12c00eac",
        os.path.join("dir", "subdir", "file"),
    )

    with dvc.state:
        used_cache = dvc.used_cache([path])
        assert (
            used_cache._items == expected._items
            and used_cache.external == expected.external
        )


def test_locked(mocker):
    repo = mocker.MagicMock()
    repo.method = locked(repo.method)

    args = {}
    kwargs = {}
    repo.method(repo, args, kwargs)

    assert repo.method_calls == [
        mocker.call._reset(),
        mocker.call.method(repo, args, kwargs),
        mocker.call._reset(),
    ]


def test_collect_optimization(tmp_dir, dvc, mocker):
    (stage,) = tmp_dir.dvc_gen("foo", "foo text")

    # Forget cached stages and graph and error out on collection
    dvc._reset()
    mocker.patch(
        "dvc.repo.Repo.stages",
        property(raiser(Exception("Should not collect"))),
    )

    # Should read stage directly instead of collecting the whole graph
    dvc.collect(stage.path)
    dvc.collect_granular(stage.path)
