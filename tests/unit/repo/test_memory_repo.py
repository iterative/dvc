import os

from dvc.repo import MemoryRepo
from dvc.utils.fs import remove


def test_memory_repo(tmp_dir, dvc, remote, run_copy):
    tmp_dir.gen({"data": {"foo": "foo", "bar": "bar"}, "lorem": "lorem"})
    run_copy("lorem", "ipsum", name="copy-lorem-ipsum", no_exec=True)

    m = MemoryRepo(tmp_dir)
    m.add("data")
    m.reproduce()

    # StageCache requires local odb.
    assert os.listdir(".dvc/cache") == ["runs"]
    m.push(run_cache=True)

    remove("data")
    remove(".dvc/cache")
    m.pull(run_cache=True)

    assert not m.status()
    assert not m.reproduce()
    assert os.listdir(".dvc/cache") == ["runs"]
