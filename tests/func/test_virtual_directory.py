import shutil
from os.path import join

from dvc_data.hashfile.hash_info import HashInfo
from dvc_data.hashfile.meta import Meta


def test_virtual_add(tmp_dir, dvc, remote):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    (stage,) = dvc.add("dir")
    out = stage.outs[0]

    assert out.hash_info == HashInfo(
        name="md5", value="5ea40360f5b4ec688df672a4db9c17d1.dir"
    )
    assert out.meta == Meta(isdir=True, size=6, nfiles=2)

    assert dvc.push() == 3
    dvc.cache.local.clear()

    tmp_dir.gen(
        {"dir": {"foobar": "foobar", "lorem": "ipsum", "subdir": {"file": "file"}}}
    )
    (stage,) = dvc.add("dir/foobar")

    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="a5beca056acbef9e0013347efdc2b751.dir"
    )
    assert out.meta == Meta(isdir=True, size=12, nfiles=3)
    assert dvc.push() == 2

    (stage,) = dvc.add("dir/subdir")
    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="de78e9fff7c3478c6b316bf08437d0f6.dir"
    )
    assert out.meta == Meta(isdir=True, size=16, nfiles=4)
    assert dvc.push() == 2


def test_virtual_remove(tmp_dir, dvc, remote):
    tmp_dir.gen(
        {
            "dir": {
                "foo": "foo",
                "bar": "bar",
                "subdir": {"lorem": "lorem", "ipsum": "ipsum"},
            }
        }
    )

    (stage,) = dvc.add("dir")
    out = stage.outs[0]

    assert out.hash_info == HashInfo(
        name="md5", value="15b0e3c73ad2c748ce206988cb6b7319.dir"
    )
    assert out.meta == Meta(isdir=True, size=16, nfiles=4)

    assert dvc.push() == 5
    dvc.cache.local.clear()

    (tmp_dir / "dir" / "foo").unlink()
    (stage,) = dvc.add("dir/foo")

    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="991ea7d558d320d8817a0798e9c676f1.dir"
    )
    assert out.meta == Meta(isdir=True, size=None, nfiles=3)

    assert dvc.push() == 1

    shutil.rmtree(tmp_dir / "dir" / "subdir")
    (stage,) = dvc.add("dir/subdir")

    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="91aaa9bb58b657d623ef143b195a67e4.dir"
    )
    assert out.meta == Meta(isdir=True, size=None, nfiles=1)
    assert dvc.push() == 1


def test_virtual_update_dir(tmp_dir, dvc, remote):
    tmp_dir.gen({"dir": {"foo": "foo", "subdir": {"lorem": "lorem"}}})
    (stage,) = dvc.add("dir")
    out = stage.outs[0]

    assert out.hash_info == HashInfo(
        name="md5", value="22a16c9bf84b3068bc2206d88a6b5776.dir"
    )
    assert out.meta == Meta(isdir=True, size=8, nfiles=2)

    assert dvc.push() == 3
    dvc.cache.local.clear()
    shutil.rmtree("dir")

    tmp_dir.gen({"dir": {"subdir": {"ipsum": "lorem ipsum", "file": "file"}}})
    (stage,) = dvc.add("dir/subdir")

    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="32f5734ea1a2aa1a067c0c15f0ae5781.dir"
    )
    assert out.meta == Meta(isdir=True, size=None, nfiles=3)
    assert dvc.push() == 3


def test_virtual_update_file(tmp_dir, dvc, remote):
    tmp_dir.gen({"dir": {"foo": "foo", "subdir": {"lorem": "lorem"}}})
    (stage,) = dvc.add("dir")
    out = stage.outs[0]

    assert out.hash_info == HashInfo(
        name="md5", value="22a16c9bf84b3068bc2206d88a6b5776.dir"
    )
    assert out.meta == Meta(isdir=True, size=8, nfiles=2)

    assert dvc.push() == 3
    dvc.cache.local.clear()
    shutil.rmtree("dir")

    tmp_dir.gen({"dir": {"foo": "foobar"}})
    (stage,) = dvc.add("dir/foo")
    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="49408ac059c76086a3a892129a324b60.dir"
    )
    assert out.meta == Meta(isdir=True, size=None, nfiles=2)
    assert dvc.push() == 2


def test_virtual_update_noop(tmp_dir, dvc, remote):
    tmp_dir.gen({"dir": {"foo": "foo", "subdir": {"lorem": "lorem"}}})

    (stage,) = dvc.add("dir")
    out = stage.outs[0]
    hash_info = HashInfo(name="md5", value="22a16c9bf84b3068bc2206d88a6b5776.dir")
    meta = Meta(isdir=True, size=8, nfiles=2)

    assert out.hash_info == hash_info
    assert out.meta == meta
    assert dvc.push() == 3

    dvc.cache.local.clear()
    shutil.rmtree("dir")

    tmp_dir.gen({"dir": {"foo": "foo", "subdir": {"lorem": "lorem"}}})

    (stage,) = dvc.add("dir/foo")
    out = stage.outs[0]
    assert out.hash_info == hash_info
    assert out.meta == meta
    assert not dvc.push()

    dvc.cache.local.clear()

    (stage,) = dvc.add("dir/subdir")
    out = stage.outs[0]
    assert out.hash_info == hash_info
    assert out.meta == meta
    assert not dvc.push()


def test_partial_checkout_and_update(M, tmp_dir, dvc, remote):
    tmp_dir.gen({"dir": {"foo": "foo", "subdir": {"lorem": "lorem"}}})

    (stage,) = dvc.add("dir")
    out = stage.outs[0]

    assert out.hash_info == HashInfo(
        name="md5", value="22a16c9bf84b3068bc2206d88a6b5776.dir"
    )
    assert out.meta == Meta(isdir=True, size=8, nfiles=2)

    assert dvc.push() == 3
    dvc.cache.local.clear()
    shutil.rmtree("dir")

    assert dvc.pull("dir/subdir") == M.dict(
        added=[join("dir", "")],
        fetched=3,
    )
    assert (tmp_dir / "dir").read_text() == {"subdir": {"lorem": "lorem"}}

    tmp_dir.gen({"dir": {"subdir": {"ipsum": "ipsum"}}})
    (stage,) = dvc.add("dir/subdir/ipsum")

    out = stage.outs[0]
    assert out.hash_info == HashInfo(
        name="md5", value="06d953a10e0b0ffacba04876a9351e39.dir"
    )
    assert out.meta == Meta(isdir=True, size=13, nfiles=3)
    assert dvc.push() == 2
