import os

from dvc.utils.fs import move, remove


class TestRemote:
    def test(self, tmp_dir, dvc, remote):  # pylint: disable=W0613
        (stage,) = tmp_dir.dvc_gen("foo", "foo")
        out = stage.outs[0]
        cache = out.cache_path
        foo_hash = out.hash_info
        foo_hashes = out.get_used_objs().get(None, set())

        (stage_dir,) = tmp_dir.dvc_gen(
            {
                "data_dir": {
                    "data_sub_dir": {"data_sub": "data_sub"},
                    "data": "data",
                    "empty": "",
                }
            }
        )

        out_dir = stage_dir.outs[0]
        cache_dir = out_dir.cache_path
        dir_hash = out_dir.hash_info
        dir_hashes = {dir_hash} | {oid for _, _, oid in out_dir.obj}

        def _check_status(status, **kwargs):
            for key in ("ok", "missing", "new", "deleted"):
                expected = kwargs.get(key, set())
                assert expected == set(getattr(status, key))

        # Check status
        status = dvc.cloud.status(foo_hashes)
        _check_status(status, new={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, new=dir_hashes)

        # Move cache and check status
        # See issue https://github.com/iterative/dvc/issues/4383 for details
        backup_dir = dvc.odb.local.cache_dir + ".backup"
        move(dvc.odb.local.cache_dir, backup_dir)
        status = dvc.cloud.status(foo_hashes)
        _check_status(status, missing={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, missing=dir_hashes)

        # Restore original cache:
        remove(dvc.odb.local.cache_dir)
        move(backup_dir, dvc.odb.local.cache_dir)

        # Push and check status
        dvc.cloud.push(foo_hashes)
        assert os.path.exists(cache)
        assert os.path.isfile(cache)

        dvc.cloud.push(dir_hashes)
        assert os.path.isfile(cache_dir)

        status = dvc.cloud.status(foo_hashes)
        _check_status(status, ok={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, ok=dir_hashes)

        # Remove and check status
        remove(dvc.odb.local.cache_dir)

        status = dvc.cloud.status(foo_hashes)
        _check_status(status, deleted={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, deleted=dir_hashes)

        # Pull and check status
        dvc.cloud.pull(foo_hashes)
        assert os.path.exists(cache)
        assert os.path.isfile(cache)
        with open(cache, encoding="utf-8") as fd:
            assert fd.read() == "foo"

        dvc.cloud.pull(dir_hashes)
        assert os.path.isfile(cache_dir)

        status = dvc.cloud.status(foo_hashes)
        _check_status(status, ok={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, ok=dir_hashes)
