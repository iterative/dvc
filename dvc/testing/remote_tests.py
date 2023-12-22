import os
import shutil

import pytest

from dvc.stage.cache import RunCacheNotSupported
from dvc.utils.fs import remove
from dvc_data.hashfile.tree import Tree


def _check_status(status, **kwargs):
    for key in ("ok", "missing", "new", "deleted"):
        expected = kwargs.get(key, set())
        assert expected == set(getattr(status, key))


class TestRemote:
    def test(self, tmp_dir, dvc, remote):
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

        # Check status
        status = dvc.cloud.status(foo_hashes)
        _check_status(status, new={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, new=dir_hashes)

        # Move cache and check status
        # See issue https://github.com/iterative/dvc/issues/4383 for details
        backup_dir = dvc.cache.local.path + ".backup"
        shutil.move(dvc.cache.local.path, backup_dir)
        status = dvc.cloud.status(foo_hashes)
        _check_status(status, missing={foo_hash})

        status_dir = dvc.cloud.status(dir_hashes)
        _check_status(status_dir, missing=dir_hashes)

        # Restore original cache:
        remove(dvc.cache.local.path)
        shutil.move(backup_dir, dvc.cache.local.path)

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
        dvc.cache.local.clear()

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

    @pytest.mark.xfail(raises=RunCacheNotSupported, strict=False)
    def test_stage_cache_push_pull(self, tmp_dir, dvc, remote):
        tmp_dir.gen("foo", "foo")
        stage = dvc.stage.add(
            deps=["foo"], outs=["bar"], name="copy-foo-bar", cmd="cp foo bar"
        )
        dvc.reproduce(stage.addressing)
        assert dvc.push(run_cache=True) == 2

        stage_cache_dir = tmp_dir / dvc.stage_cache.cache_dir
        expected = list(stage_cache_dir.rglob("*"))
        shutil.rmtree(stage_cache_dir)

        dvc.pull(run_cache=True)
        assert list(stage_cache_dir.rglob("*")) == expected

    @pytest.mark.xfail(raises=NotImplementedError, strict=False)
    def test_pull_00_prefix(self, tmp_dir, dvc, remote, monkeypatch):
        # Related: https://github.com/iterative/dvc/issues/6089

        fs_type = type(dvc.cloud.get_remote_odb("upstream").fs)
        monkeypatch.setattr(fs_type, "_ALWAYS_TRAVERSE", True, raising=False)
        monkeypatch.setattr(fs_type, "LIST_OBJECT_PAGE_SIZE", 256, raising=False)

        # foo's md5 checksum is 00411460f7c92d2124a67ea0f4cb5f85
        # bar's md5 checksum is 0000000018e6137ac2caab16074784a6
        foo_out = tmp_dir.dvc_gen("foo", "363")[0].outs[0]
        bar_out = tmp_dir.dvc_gen("bar", "jk8ssl")[0].outs[0]
        expected_hashes = {foo_out.hash_info, bar_out.hash_info}

        dvc.push()
        status = dvc.cloud.status(expected_hashes)
        _check_status(status, ok=expected_hashes)

        dvc.cache.local.clear()
        remove(tmp_dir / "foo")
        remove(tmp_dir / "bar")

        stats = dvc.pull()
        assert stats["fetched"] == 2
        assert set(stats["added"]) == {"foo", "bar"}

    @pytest.mark.xfail(raises=NotImplementedError, strict=False)
    def test_pull_no_00_prefix(self, tmp_dir, dvc, remote, monkeypatch):
        # Related: https://github.com/iterative/dvc/issues/6244

        fs_type = type(dvc.cloud.get_remote_odb("upstream").fs)
        monkeypatch.setattr(fs_type, "_ALWAYS_TRAVERSE", True, raising=False)
        monkeypatch.setattr(fs_type, "LIST_OBJECT_PAGE_SIZE", 256, raising=False)

        # foo's md5 checksum is 14ffd92a6cbf5f2f657067df0d5881a6
        # bar's md5 checksum is 64020400f00960c0ef04052547b134b3
        foo_out = tmp_dir.dvc_gen("foo", "dvc")[0].outs[0]
        bar_out = tmp_dir.dvc_gen("bar", "cml")[0].outs[0]
        expected_hashes = {foo_out.hash_info, bar_out.hash_info}

        dvc.push()
        status = dvc.cloud.status(expected_hashes)
        _check_status(status, ok=expected_hashes)

        dvc.cache.local.clear()
        remove(tmp_dir / "foo")
        remove(tmp_dir / "bar")

        stats = dvc.pull()
        assert stats["fetched"] == 2
        assert set(stats["added"]) == {"foo", "bar"}


class TestRemoteVersionAware:
    def test_file(self, tmp_dir, dvc, run_copy, remote_version_aware):
        (stage,) = tmp_dir.dvc_gen("foo", "foo")
        run_copy("foo", "foo_copy", name="copy")

        assert dvc.push()
        assert (remote_version_aware / "foo").read_text() == "foo"
        assert (remote_version_aware / "foo_copy").read_text() == "foo"
        foo_dvc = (tmp_dir / "foo.dvc").read_text()
        assert "version_id" in foo_dvc
        stage = stage.reload()
        out = stage.outs[0]
        assert out.meta.version_id
        dvc_lock = (tmp_dir / "dvc.lock").read_text()

        remove(dvc.cache.local.path)
        remove(tmp_dir / "foo")
        remove(tmp_dir / "foo_copy")

        assert dvc.pull()
        assert (tmp_dir / "foo").read_text() == "foo"
        assert (tmp_dir / "foo_copy").read_text() == "foo"
        assert (tmp_dir / "foo.dvc").read_text() == foo_dvc
        assert (tmp_dir / "dvc.lock").read_text() == dvc_lock

        assert not dvc.push()
        assert (remote_version_aware / "foo").read_text() == "foo"
        assert (remote_version_aware / "foo_copy").read_text() == "foo"
        assert (tmp_dir / "foo.dvc").read_text() == foo_dvc
        assert (tmp_dir / "dvc.lock").read_text() == dvc_lock

        dvc.reproduce()
        assert not dvc.push()
        assert (remote_version_aware / "foo").read_text() == "foo"
        assert (remote_version_aware / "foo_copy").read_text() == "foo"
        assert (tmp_dir / "foo.dvc").read_text() == foo_dvc
        assert (tmp_dir / "dvc.lock").read_text() == dvc_lock

    def test_dir(self, tmp_dir, dvc, run_copy, remote_version_aware):  # noqa: PLR0915
        (stage,) = tmp_dir.dvc_gen(
            {
                "data_dir": {
                    "data_sub_dir": {"data_sub": "data_sub"},
                    "data": "data",
                    "empty": "",
                }
            }
        )

        assert not dvc.fetch()
        assert dvc.push()

        data_dir_dvc = (tmp_dir / "data_dir.dvc").read_text()
        assert "files" in data_dir_dvc
        assert "version_id" in data_dir_dvc
        stage = stage.reload()
        out = stage.outs[0]
        assert out.files
        for file in out.files:
            assert file["version_id"]
            assert file["remote"] == "upstream"

        remove(dvc.cache.local.path)
        remove(tmp_dir / "data_dir")

        assert dvc.pull()
        assert (tmp_dir / "data_dir" / "data").read_text() == "data"
        assert (
            tmp_dir / "data_dir" / "data_sub_dir" / "data_sub"
        ).read_text() == "data_sub"
        assert (tmp_dir / "data_dir.dvc").read_text() == data_dir_dvc

        run_copy("data_dir", "data_dir_copy", name="copy")
        dvc_lock = (tmp_dir / "dvc.lock").read_text()

        assert dvc.push()
        assert (remote_version_aware / "data_dir").exists()
        assert (remote_version_aware / "data_dir" / "data").exists()
        assert (remote_version_aware / "data_dir_copy").exists()
        assert (remote_version_aware / "data_dir_copy" / "data").exists()
        assert (tmp_dir / "data_dir.dvc").read_text() == data_dir_dvc
        assert (tmp_dir / "dvc.lock").read_text() != dvc_lock
        dvc_lock = (tmp_dir / "dvc.lock").read_text()

        assert not dvc.push()
        assert (remote_version_aware / "data_dir").exists()
        assert (remote_version_aware / "data_dir" / "data").exists()
        assert (remote_version_aware / "data_dir_copy").exists()
        assert (remote_version_aware / "data_dir_copy" / "data").exists()
        assert (tmp_dir / "data_dir.dvc").read_text() == data_dir_dvc
        assert (tmp_dir / "dvc.lock").read_text() == dvc_lock

        dvc.cache.local.clear()
        remove(tmp_dir / "data_dir")
        remove(tmp_dir / "data_dir_copy")
        assert not dvc.push()
        assert (remote_version_aware / "data_dir").exists()
        assert (remote_version_aware / "data_dir" / "data").exists()
        assert (remote_version_aware / "data_dir_copy").exists()
        assert (remote_version_aware / "data_dir_copy" / "data").exists()
        assert (tmp_dir / "data_dir.dvc").read_text() == data_dir_dvc
        assert (tmp_dir / "dvc.lock").read_text() == dvc_lock

        (remote_version_aware / "data_dir").rmdir()
        (remote_version_aware / "data_dir_copy").rmdir()
        assert not (remote_version_aware / "data_dir").exists()
        assert not (remote_version_aware / "data_dir_copy").exists()
        assert dvc.pull()
        assert (tmp_dir / "data_dir" / "data").read_text() == "data"
        assert (
            tmp_dir / "data_dir" / "data_sub_dir" / "data_sub"
        ).read_text() == "data_sub"
        assert (tmp_dir / "data_dir_copy" / "data").read_text() == "data"
        assert (
            tmp_dir / "data_dir_copy" / "data_sub_dir" / "data_sub"
        ).read_text() == "data_sub"


class TestRemoteWorktree:
    def test_file(self, tmp_dir, dvc, remote_worktree):
        (stage,) = tmp_dir.dvc_gen("foo", "foo")

        dvc.push()
        assert "version_id" in (tmp_dir / "foo.dvc").read_text()
        stage = stage.reload()
        out = stage.outs[0]
        assert out.meta.version_id

        remove(dvc.cache.local.path)
        remove(tmp_dir / "foo")

        dvc.pull()
        assert (tmp_dir / "foo").read_text() == "foo"

    def test_dir(self, tmp_dir, dvc, remote_worktree):
        (stage,) = tmp_dir.dvc_gen(
            {
                "data_dir": {
                    "data_sub_dir": {"data_sub": "data_sub"},
                    "data": "data",
                    "empty": "",
                }
            }
        )

        dvc.push()
        assert "files" in (tmp_dir / "data_dir.dvc").read_text()
        assert "version_id" in (tmp_dir / "data_dir.dvc").read_text()
        stage = stage.reload()
        out = stage.outs[0]
        assert out.files
        for file in out.files:
            assert file["version_id"]
            assert file["remote"] == "upstream"

        remove(dvc.cache.local.path)
        remove(tmp_dir / "data_dir")

        dvc.pull()
        assert (tmp_dir / "data_dir" / "data").read_text() == "data"
        assert (
            tmp_dir / "data_dir" / "data_sub_dir" / "data_sub"
        ).read_text() == "data_sub"

    def test_deletion(self, tmp_dir, dvc, scm, remote_worktree):
        tmp_dir.dvc_gen(
            {
                "data_dir": {
                    "data_sub_dir": {"data_sub": "data_sub"},
                    "data": "data",
                    "empty": "",
                }
            }
        )
        dvc.push()
        assert (remote_worktree / "data_dir" / "data").exists()
        tmp_dir.scm_add([tmp_dir / "data_dir.dvc"], commit="v1")
        v1 = scm.get_rev()
        remove(tmp_dir / "data_dir" / "data")
        dvc.add(str(tmp_dir / "data_dir"))

        # data_dir/data should show as deleted in the remote
        dvc.push()
        tmp_dir.scm_add([tmp_dir / "data_dir.dvc"], commit="v2")
        assert not (remote_worktree / "data_dir" / "data").exists()

        remove(dvc.cache.local.path)
        remove(tmp_dir / "data_dir")
        # pulling the original pushed version should still succeed
        scm.checkout(v1)
        dvc.pull()
        assert (tmp_dir / "data_dir" / "data").read_text() == "data"

    def test_update(self, tmp_dir, dvc, remote_worktree):
        (foo_stage,) = tmp_dir.dvc_gen("foo", "foo")
        (data_dir_stage,) = tmp_dir.dvc_gen(
            {
                "data_dir": {
                    "data_sub_dir": {"data_sub": "data_sub"},
                    "data": "data",
                    "empty": "",
                }
            }
        )
        dvc.push()
        orig_foo = foo_stage.reload().outs[0]
        orig_data_dir = data_dir_stage.reload().outs[0]
        (remote_worktree / "foo").write_text("bar")
        (remote_worktree / "data_dir" / "data").write_text("modified")
        (remote_worktree / "data_dir" / "new_data").write_text("new data")

        dvc.update([str(tmp_dir / "foo.dvc"), str(tmp_dir / "data_dir.dvc")])
        updated_foo = foo_stage.reload().outs[0]
        updated_data_dir = data_dir_stage.reload().outs[0]

        assert updated_foo.meta.version_id
        assert updated_foo.meta.version_id != orig_foo.meta.version_id
        updated_data_dir = data_dir_stage.reload().outs[0]
        orig_tree = orig_data_dir.get_obj()
        updated_tree = Tree.from_list(updated_data_dir.files, hash_name="md5")
        assert orig_tree.get(("data_sub_dir", "data_sub")) == updated_tree.get(
            ("data_sub_dir", "data_sub")
        )
        orig_meta, _ = orig_tree.get(("data",))
        updated_meta, _ = updated_tree.get(("data",))
        assert orig_meta.version_id
        assert updated_meta.version_id
        assert orig_meta.version_id != updated_meta.version_id
        meta, hash_info = updated_tree.get(("new_data",))
        assert meta
        assert hash_info

        assert (tmp_dir / "foo").read_text() == "bar"
        assert (tmp_dir / "data_dir" / "data").read_text() == "modified"
        assert (tmp_dir / "data_dir" / "new_data").read_text() == "new data"

        remove(dvc.cache.local.path)
        remove(tmp_dir / "foo")
        remove(tmp_dir / "data_dir")
        dvc.pull()
        assert (tmp_dir / "foo").read_text() == "bar"
        assert (tmp_dir / "data_dir" / "data").read_text() == "modified"
        assert (tmp_dir / "data_dir" / "new_data").read_text() == "new data"
