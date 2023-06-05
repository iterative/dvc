import os


def test_unprotect(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")

    dvc.cache.local.cache_types = ["hardlink"]
    dvc.add("foo")
    cache = os.path.join(
        ".dvc", "cache", "files", "md5", "ac", "bd18db4cc2f85cedef654fccc4a4d8"
    )
    assert not os.access("foo", os.W_OK)
    assert not os.access(cache, os.W_OK)

    dvc.unprotect("foo")
    assert os.access("foo", os.W_OK)

    if os.name == "nt":
        # NOTE: cache is now unprotected, because NTFS doesn't allow
        # deleting read-only files, so we have to try to set write perms
        # on files that we try to delete, which propagates to the cache
        # file. But it should be restored after the next cache check, hence
        # why we call `dvc status` here.
        assert os.access(cache, os.W_OK)
        dvc.status()

    assert not os.access(cache, os.W_OK)
