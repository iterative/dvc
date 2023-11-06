import os


def test_du(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "file": b"file",
            "dvcfile": b"dvcfile",
            "dir": {
                "dirfile": b"dirfile",
                "subdir": {
                    "subdirfile": b"subdirfile",
                },
                "dvcsubdir": {
                    "dvcsubdirfile": b"dvcsubdirfile",
                },
            },
        }
    )

    dvc.add("dvcfile")
    dvc.add(os.path.join("dir", "dvcsubdir"))

    assert dvc.du(".", "file") == [("file", 4)]
    assert dvc.du(".", "dvcfile") == [("dvcfile", 7)]
    assert set(dvc.du(".", "dir/subdir")) == {
        ("dir/subdir/subdirfile", 10),
        ("dir/subdir", 10),
    }
    assert dvc.du(".", "dir/subdir", summarize=True) == [("dir/subdir", 10)]
    assert set(dvc.du(".", "dir/dvcsubdir")) == {
        ("dir/dvcsubdir/dvcsubdirfile", 13),
        ("dir/dvcsubdir", 13),
    }
    assert dvc.du(".", "dir/dvcsubdir", summarize=True) == [("dir/dvcsubdir", 13)]
    assert set(dvc.du(".", "dir")) == {
        ("dir/dvcsubdir", 13),
        ("dir/subdir", 10),
        ("dir/dirfile", 7),
        ("dir", 30),
    }
    assert dvc.du(".", "dir", summarize=True) == [("dir", 30)]
    assert set(dvc.du(".", "/")) == {
        ("/dvcfile", 7),
        ("/dir", 30),
        ("/file", 4),
        ("/", 41),
    }
    assert dvc.du(".", "/", summarize=True) == [("/", 41)]
