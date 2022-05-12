import shutil

from .tmp_dir import TmpDir


def test_push_pull(tmp_dir, dvc, remote):
    tmp_dir.gen("foo", "foo")
    stage = dvc.stage.add(
        deps=["foo"], outs=["bar"], name="copy-foo-bar", cmd="cp foo bar"
    )
    dvc.reproduce(stage.addressing)
    assert dvc.push(run_cache=True) == 2

    stage_cache_dir = TmpDir(dvc.stage_cache.cache_dir)
    expected = list(stage_cache_dir.rglob("*"))
    shutil.rmtree(stage_cache_dir)

    dvc.pull(run_cache=True)
    assert list(stage_cache_dir.rglob("*")) == expected
