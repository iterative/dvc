import filecmp
import os

import pytest
from funcy import first

from dvc.cachemgr import CacheManager
from dvc.config import NoRemoteError
from dvc.dependency import base
from dvc.dvcfile import load_file
from dvc.fs import system
from dvc.scm import Git
from dvc.stage.exceptions import StagePathNotFoundError
from dvc.testing.tmp_dir import make_subrepo
from dvc.utils.fs import remove
from dvc_data.index.index import DataIndexDirError


def test_import(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    stage = dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported")

    assert os.path.isfile("foo_imported")
    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.is_ignored("foo_imported")
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }
    assert stage.deps[0].fs.repo.cache.local.path == dvc.cache.local.path


@pytest.mark.parametrize("src_is_dvc", [True, False])
def test_import_git_file(tmp_dir, scm, dvc, git_dir, src_is_dvc):
    if src_is_dvc:
        git_dir.init(dvc=True)

    git_dir.scm_gen("src", "hello", commit="add a git file")

    stage = tmp_dir.dvc.imp(os.fspath(git_dir), "src", "dst")

    assert (tmp_dir / "dst").read_text() == "hello"
    assert tmp_dir.scm.is_ignored(os.fspath(tmp_dir / "dst"))
    assert stage.deps[0].def_repo == {
        "url": os.fspath(git_dir),
        "rev_lock": git_dir.scm.get_rev(),
    }


def test_import_cached_file(mocker, erepo_dir, tmp_dir, dvc, scm, monkeypatch):
    src = "some_file"
    dst = "some_file_imported"

    with erepo_dir.chdir():
        erepo_dir.dvc_gen({src: "hello"}, commit="add a regular file")

    tmp_dir.dvc_gen({dst: "hello"})
    (tmp_dir / dst).unlink()

    remote_exception = NoRemoteError("dvc import")
    mocker.patch.object(dvc.cloud, "get_remote_odb", side_effect=remote_exception)
    tmp_dir.dvc.imp(os.fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(erepo_dir / src, tmp_dir / dst, shallow=False)


@pytest.mark.parametrize("src_is_dvc", [True, False])
def test_import_git_dir(tmp_dir, scm, dvc, git_dir, src_is_dvc):
    if src_is_dvc:
        git_dir.init(dvc=True)

    git_dir.scm_gen({"src": {"file.txt": "hello"}}, commit="add a dir")

    stage = dvc.imp(os.fspath(git_dir), "src", "dst")

    assert (tmp_dir / "dst").read_text() == {"file.txt": "hello"}
    assert tmp_dir.scm.is_ignored(os.fspath(tmp_dir / "dst"))
    assert stage.deps[0].def_repo == {
        "url": os.fspath(git_dir),
        "rev_lock": git_dir.scm.get_rev(),
    }


def test_import_dir(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    stage = dvc.imp(os.fspath(erepo_dir), "dir", "dir_imported")

    assert (tmp_dir / "dir_imported").read_text() == {"foo": "foo content"}
    assert scm.is_ignored("dir_imported")
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_import_broken_dir(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")
        erepo_dir.dvc.cache.local.clear()
        remove(erepo_dir / "dir")

    with pytest.raises(DataIndexDirError):
        dvc.imp(os.fspath(erepo_dir), "dir", "dir_imported")

    assert not (tmp_dir / "dir_imported").exists()
    assert not (tmp_dir / "dir_imported.dvc").exists()


def test_import_file_from_dir(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir": {
                    "1": "1",
                    "2": "2",
                    "subdir": {"foo": "foo", "bar": "bar"},
                }
            },
            commit="create dir",
        )

    stage = dvc.imp(os.fspath(erepo_dir), os.path.join("dir", "1"))

    assert (tmp_dir / "1").read_text() == "1"
    assert scm.is_ignored("1")
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }

    dvc.imp(os.fspath(erepo_dir), os.path.join("dir", "2"), out="file")
    assert (tmp_dir / "file").read_text() == "2"
    assert (tmp_dir / "file.dvc").exists()

    dvc.imp(os.fspath(erepo_dir), os.path.join("dir", "subdir"))
    assert (tmp_dir / "subdir" / "foo").read_text() == "foo"
    assert (tmp_dir / "subdir" / "bar").read_text() == "bar"
    assert (tmp_dir / "subdir.dvc").exists()

    dvc.imp(os.fspath(erepo_dir), os.path.join("dir", "subdir", "foo"), out="X")
    assert (tmp_dir / "X").read_text() == "foo"
    assert (tmp_dir / "X.dvc").exists()


def test_import_file_from_dir_to_dir(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="create dir")

    with pytest.raises(StagePathNotFoundError):
        dvc.imp(
            os.fspath(erepo_dir),
            os.path.join("dir", "foo"),
            out=os.path.join("dir", "foo"),
        )

    tmp_dir.gen({"dir": {}})
    dvc.imp(
        os.fspath(erepo_dir),
        os.path.join("dir", "foo"),
        out=os.path.join("dir", "foo"),
    )
    assert not (tmp_dir / "foo.dvc").exists()
    assert (tmp_dir / "dir" / "foo").read_text() == "foo"
    assert (tmp_dir / "dir" / "foo.dvc").exists()


def test_import_non_cached(erepo_dir, tmp_dir, dvc, scm):
    src = "non_cached_output"
    dst = src + "_imported"

    with erepo_dir.chdir():
        erepo_dir.dvc.run(
            cmd=f"echo hello > {src}",
            outs_no_cache=[src],
            name="gen",
        )

    erepo_dir.scm_add([os.fspath(erepo_dir / src)], commit="add a non-cached out")

    stage = tmp_dir.dvc.imp(os.fspath(erepo_dir), src, dst)

    assert (tmp_dir / dst).is_file()
    assert filecmp.cmp(erepo_dir / src, tmp_dir / dst, shallow=False)
    assert tmp_dir.scm.is_ignored(dst)
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_import_rev(tmp_dir, scm, dvc, erepo_dir):
    rev = None
    with erepo_dir.chdir(), erepo_dir.branch("branch", new=True):
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo on branch")
        rev = erepo_dir.scm.get_rev()

    stage = dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", rev="branch")

    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.is_ignored("foo_imported")
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev": "branch",
        "rev_lock": rev,
    }


def test_pull_imported_stage(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")
    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported")

    dst_stage = load_file(dvc, "foo_imported.dvc").stage
    dst_cache = dst_stage.outs[0].cache_path

    remove("foo_imported")
    remove(dst_cache)
    dvc.pull(["foo_imported.dvc"])

    assert os.path.isfile("foo_imported")
    assert os.path.isfile(dst_cache)


def test_import_no_download(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", no_download=True)

    assert not os.path.exists("foo_imported")

    dst_stage = load_file(dvc, "foo_imported.dvc").stage

    assert dst_stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }
    assert scm.is_ignored("foo_imported")


def test_pull_import_no_download(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm_gen(os.path.join("foo", "bar"), b"bar", commit="add bar")
        erepo_dir.dvc_gen(os.path.join("foo", "baz"), b"baz contents", commit="add baz")
        size = (
            len(b"bar")
            + len(b"baz contents")
            + len((erepo_dir / "foo" / ".gitignore").read_bytes())
        )

    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", no_download=True)

    dvc.pull(["foo_imported.dvc"])
    assert (tmp_dir / "foo_imported").exists()
    assert (tmp_dir / "foo_imported" / "bar").read_bytes() == b"bar"
    assert (tmp_dir / "foo_imported" / "baz").read_bytes() == b"baz contents"

    dvc.commit(force=True)

    stage = load_file(dvc, "foo_imported.dvc").stage
    if os.name == "nt":
        expected_hash = "2e798234df5f782340ac3ce046f8dfae.dir"
    else:
        expected_hash = "bdb8641831d8fcb03939637e09011c21.dir"
    assert stage.outs[0].hash_info.value == expected_hash

    assert stage.outs[0].meta.size == size
    assert stage.outs[0].meta.nfiles == 3
    assert stage.outs[0].meta.isdir


def test_pull_import_no_download_rev_lock(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="add")

    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", no_download=True)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "modified foo content", commit="modify foo")

    dvc.pull(["foo_imported.dvc"])
    assert (tmp_dir / "foo_imported").read_text() == "foo content"


def test_cache_type_is_properly_overridden(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        with erepo_dir.dvc.config.edit() as conf:
            conf["cache"]["type"] = "symlink"
        erepo_dir.dvc.cache = CacheManager(erepo_dir.dvc)
        erepo_dir.scm_add(
            [erepo_dir.dvc.config.files["repo"]],
            "set source repo cache type to symlink",
        )
        erepo_dir.dvc_gen("foo", "foo content", "create foo")
    assert system.is_symlink(erepo_dir / "foo")

    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported")

    assert not system.is_symlink("foo_imported")
    assert (tmp_dir / "foo_imported").read_text() == "foo content"
    assert scm.is_ignored("foo_imported")


@pytest.mark.parametrize("dirpath", ["dir", "dir/"])
def test_pull_imported_directory_stage(tmp_dir, dvc, erepo_dir, dirpath):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo content"}}, commit="create dir")

    stage = dvc.imp(os.fspath(erepo_dir), dirpath, "dir_imported")

    remove("dir_imported")
    dvc.cache.local.clear()

    dvc.pull(["dir_imported.dvc"])

    assert (tmp_dir / "dir_imported").read_text() == {"foo": "foo content"}
    assert stage.deps[0].fs_path == "dir"


def test_pull_wildcard_imported_directory_stage(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir123": {"foo": "foo content"}}, commit="create dir")

    dvc.imp(os.fspath(erepo_dir), "dir123", "dir_imported123")

    remove("dir_imported123")
    dvc.cache.local.clear()

    dvc.pull(["dir_imported*.dvc"], glob=True)

    assert (tmp_dir / "dir_imported123").read_text() == {"foo": "foo content"}


def test_push_wildcard_from_bare_git_repo(
    tmp_dir, make_tmp_dir, erepo_dir, local_cloud
):
    Git.init(tmp_dir.fs_path, bare=True).close()

    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir123": {"foo": "foo content"},
                "dirextra": {"extrafoo": "extra foo content"},
            },
            commit="initial",
        )
    erepo_dir.dvc.push([os.path.join(os.fspath(erepo_dir), "dire*")], glob=True)

    erepo_dir.scm.gitpython.repo.create_remote("origin", os.fspath(tmp_dir))
    erepo_dir.scm.gitpython.repo.remote("origin").push("master")

    dvc_repo = make_tmp_dir("dvc-repo", scm=True, dvc=True)
    with dvc_repo.chdir():
        dvc_repo.dvc.imp(os.fspath(tmp_dir), "dirextra")

        with pytest.raises(DataIndexDirError):
            dvc_repo.dvc.imp(os.fspath(tmp_dir), "dir123")


@pytest.mark.parametrize("dname", [".", "dir", "dir/subdir"])
def test_import_to_dir(dname, tmp_dir, dvc, erepo_dir):
    os.makedirs(dname, exist_ok=True)

    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    stage = dvc.imp(os.fspath(erepo_dir), "foo", dname)

    dst = os.path.join(dname, "foo")

    assert stage.outs[0].fspath == os.path.abspath(dst)
    assert os.path.isdir(dname)
    assert (tmp_dir / dst).read_text() == "foo content"


def test_pull_non_workspace(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "master content", commit="create foo")

        with erepo_dir.branch("branch", new=True):
            erepo_dir.dvc_gen("foo", "branch content", commit="modify foo")

    stage = dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", rev="branch")
    tmp_dir.scm_add([stage.relpath], commit="imported branch")
    scm.tag("ref-to-branch")

    # Overwrite via import
    (tmp_dir / "foo_imported").unlink()
    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", rev="master")

    remove(stage.outs[0].cache_path)
    dvc.fetch(all_tags=True)
    assert os.path.exists(stage.outs[0].cache_path)


def test_import_non_existing(erepo_dir, tmp_dir, dvc):
    with pytest.raises(FileNotFoundError):
        tmp_dir.dvc.imp(os.fspath(erepo_dir), "invalid_output")

    # https://github.com/iterative/dvc/pull/2837#discussion_r352123053
    with pytest.raises(FileNotFoundError):
        tmp_dir.dvc.imp(os.fspath(erepo_dir), "/root/", "root")


def test_pull_no_rev_lock(erepo_dir, tmp_dir, dvc):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "contents", commit="create foo")

    stage = dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported")
    assert "rev" not in stage.deps[0].def_repo
    stage.deps[0].def_repo.pop("rev_lock")

    load_file(dvc, stage.path).dump(stage)

    remove(stage.outs[0].cache_path)
    (tmp_dir / "foo_imported").unlink()

    dvc.pull([stage.path])

    assert (tmp_dir / "foo_imported").is_file()
    assert (tmp_dir / "foo_imported").read_text() == "contents"


def test_import_from_bare_git_repo(tmp_dir, make_tmp_dir, erepo_dir, local_cloud):
    Git.init(tmp_dir.fs_path, bare=True).close()

    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"foo": "foo"}, commit="initial")
    erepo_dir.dvc.push()

    erepo_dir.scm.gitpython.repo.create_remote("origin", os.fspath(tmp_dir))
    erepo_dir.scm.gitpython.repo.remote("origin").push("master")

    dvc_repo = make_tmp_dir("dvc-repo", scm=True, dvc=True)
    with dvc_repo.chdir():
        dvc_repo.dvc.imp(os.fspath(tmp_dir), "foo")


def test_import_pipeline_tracked_outs(
    tmp_dir, dvc, scm, erepo_dir, run_copy, local_remote
):
    from dvc.dvcfile import LOCK_FILE, PROJECT_FILE

    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    dvc.push()

    dvc.scm.add([PROJECT_FILE, LOCK_FILE])
    dvc.scm.commit("add pipeline stage")

    with erepo_dir.chdir():
        erepo_dir.dvc.imp(f"file://{tmp_dir.as_posix()}", "bar", out="baz")
        assert (erepo_dir / "baz").read_text() == "foo"


def test_local_import(tmp_dir, dvc, scm):
    tmp_dir.dvc_gen("foo", "foo", commit="init")
    (tmp_dir / "outdir").mkdir()
    dvc.imp(".", "foo", out="outdir")


def test_import_mixed_dir(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen(os.path.join("dir", "foo"), "foo", commit="foo")
        erepo_dir.scm_gen(os.path.join("dir", "bar"), "bar", commit="bar")

    dvc.imp(os.fspath(erepo_dir), "dir")
    assert (tmp_dir / "dir").read_text() == {
        ".gitignore": "/foo\n",
        "foo": "foo",
        "bar": "bar",
    }


@pytest.mark.parametrize("is_dvc", [True, False])
@pytest.mark.parametrize("files", [{"foo": "foo"}, {"dir": {"bar": "bar"}}])
def test_import_subrepos(tmp_dir, erepo_dir, dvc, scm, is_dvc, files):
    subrepo = erepo_dir / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    gen = subrepo.dvc_gen if is_dvc else subrepo.scm_gen
    with subrepo.chdir():
        gen(files, commit="add files in subrepo")

    key = next(iter(files))
    path = str((subrepo / key).relative_to(erepo_dir))

    stage = dvc.imp(os.fspath(erepo_dir), path, out="out")

    assert (tmp_dir / "out").read_text() == files[key]
    assert stage.deps[0].def_path == path
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


def test_granular_import_from_subrepos(tmp_dir, dvc, erepo_dir):
    subrepo = erepo_dir / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    with subrepo.chdir():
        subrepo.dvc_gen({"dir": {"bar": "bar"}}, commit="files in subrepo")

    path = os.path.join("subrepo", "dir", "bar")
    stage = dvc.imp(os.fspath(erepo_dir), path, out="out")
    assert (tmp_dir / "out").read_text() == "bar"
    assert stage.deps[0].def_path == path
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


@pytest.mark.parametrize("is_dvc", [True, False])
@pytest.mark.parametrize("files", [{"foo": "foo"}, {"dir": {"bar": "bar"}}])
def test_pull_imported_stage_from_subrepos(tmp_dir, dvc, erepo_dir, is_dvc, files):
    subrepo = erepo_dir / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    gen = subrepo.dvc_gen if is_dvc else subrepo.scm_gen
    with subrepo.chdir():
        gen(files, commit="files in subrepo")

    key = first(files)
    path = os.path.join("subrepo", key)
    dvc.imp(os.fspath(erepo_dir), path, out="out")

    # clean everything
    dvc.cache.local.clear()
    remove("out")

    stats = dvc.pull(["out.dvc"])

    expected = [f"out{os.sep}"] if isinstance(files[key], dict) else ["out"]
    assert stats["added"] == expected
    assert (tmp_dir / "out").read_text() == files[key]


def test_import_complete_repo(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"foo": "foo"}, commit="add foo")

    subrepo = erepo_dir / "subrepo"
    make_subrepo(subrepo, erepo_dir.scm)
    with subrepo.chdir():
        subrepo.dvc_gen({"dir": {"bar": "bar"}}, commit="files in subrepo")

    dvc.imp(os.fspath(erepo_dir), "subrepo", out="out_sub")
    assert (tmp_dir / "out_sub").read_text() == {
        ".gitignore": "/dir\n",
        "dir": {"bar": "bar"},
    }

    dvc.imp(os.fspath(erepo_dir), os.curdir, out="out")
    assert (tmp_dir / "out").read_text() == {".gitignore": "/foo\n", "foo": "foo"}


def test_import_with_no_exec(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    dvc.imp(os.fspath(erepo_dir), "foo", out="foo_imported", no_exec=True)

    dst = tmp_dir / "foo_imported"
    assert not dst.exists()


def test_import_with_jobs(mocker, dvc, erepo_dir):
    import dvc_data.hashfile.transfer as otransfer

    with erepo_dir.chdir():
        erepo_dir.dvc_gen(
            {
                "dir1": {
                    "file1": "file1",
                    "file2": "file2",
                    "file3": "file3",
                    "file4": "file4",
                }
            },
            commit="init",
        )

    spy = mocker.spy(otransfer, "transfer")
    dvc.imp(os.fspath(erepo_dir), "dir1", jobs=3)
    # the first call will be retrieving dir cache for "dir1" w/jobs None
    for _args, kwargs in spy.call_args_list[1:]:
        assert kwargs.get("jobs") == 3


def test_chained_import(tmp_dir, dvc, make_tmp_dir, erepo_dir, local_cloud):
    erepo_dir.add_remote(config=local_cloud.config)
    with erepo_dir.chdir():
        erepo_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}}, commit="init")
    erepo_dir.dvc.push()
    remove(erepo_dir.dvc.cache.local.path)
    remove(os.fspath(erepo_dir / "dir"))

    erepo2 = make_tmp_dir("erepo2", scm=True, dvc=True)
    with erepo2.chdir():
        erepo2.dvc.imp(os.fspath(erepo_dir), "dir")
        erepo2.scm.add("dir.dvc")
        erepo2.scm.commit("import")
    remove(erepo2.dvc.cache.local.path)
    remove(os.fspath(erepo2 / "dir"))

    dvc.imp(os.fspath(erepo2), "dir", "dir_imported")
    dst = tmp_dir / "dir_imported"
    assert (dst / "foo").read_text() == "foo"
    assert (dst / "bar").read_text() == "bar"

    dvc.cache.local.clear()
    remove("dir_imported")

    # pulled objects should come from the original upstream repo's remote,
    # no cache or remote should be needed from the intermediate repo
    dvc.pull(["dir_imported.dvc"])
    assert not os.path.exists(erepo_dir.dvc.cache.local.path)
    assert not os.path.exists(erepo2.dvc.cache.local.path)
    assert (dst / "foo").read_text() == "foo"
    assert (dst / "bar").read_text() == "bar"


@pytest.mark.parametrize("paths", ([], ["dir"]))
def test_parameterized_repo(tmp_dir, dvc, scm, erepo_dir, paths):
    path = erepo_dir.joinpath(*paths)
    path.mkdir(parents=True, exist_ok=True)
    (path / "params.yaml").dump({"out": "foo"})
    (path / "dvc.yaml").dump(
        {
            "stages": {
                "train": {"cmd": "echo ${out} > ${out}", "outs": ["${out}"]},
            }
        }
    )
    path.gen({"foo": "foo"})
    with path.chdir():
        erepo_dir.dvc.commit(None, force=True)
        erepo_dir.scm.add_commit(
            ["params.yaml", "dvc.yaml", "dvc.lock", ".gitignore"],
            message="init",
        )

    to_import = os.path.join(*paths, "foo")
    stage = dvc.imp(os.fspath(erepo_dir), to_import, "foo_imported")

    assert (tmp_dir / "foo_imported").read_text() == "foo"
    assert stage.deps[0].def_repo == {
        "url": os.fspath(erepo_dir),
        "rev_lock": erepo_dir.scm.get_rev(),
    }


@pytest.mark.parametrize(
    "options, def_repo",
    [
        ({"config": "myconfig"}, {"config": "myconfig"}),
        ({"remote": "myremote"}, {"remote": "myremote"}),
        ({"remote_config": {"key": "value"}}, {"remote": {"key": "value"}}),
        (
            {
                "remote": "myremote",
                "remote_config": {"key": "value"},
            },
            {
                "config": {
                    "core": {"remote": "myremote"},
                    "remote": {
                        "myremote": {"key": "value"},
                    },
                },
            },
        ),
        (
            {
                "remote": "myremote",
                "remote_config": {"key": "value"},
                "config": {"otherkey": "othervalue"},
            },
            {
                "config": {
                    "core": {"remote": "myremote"},
                    "remote": {
                        "myremote": {"key": "value"},
                    },
                    "otherkey": "othervalue",
                },
            },
        ),
    ],
)
def test_import_configs(tmp_dir, scm, dvc, erepo_dir, options, def_repo):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    (tmp_dir / "myconfig").touch()

    stage = dvc.imp(
        os.fspath(erepo_dir), "foo", "foo_imported", no_exec=True, **options
    )
    assert stage.deps[0].def_repo == {"url": os.fspath(erepo_dir), **def_repo}


def test_import_invalid_configs(tmp_dir, scm, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    with pytest.raises(
        ValueError,
        match="Can't specify config path together with both remote and remote_config",
    ):
        dvc.imp(
            os.fspath(erepo_dir),
            "foo",
            "foo_imported",
            no_exec=True,
            config="myconfig",
            remote="myremote",
            remote_config={"key": "value"},
        )


def test_reimport(tmp_dir, scm, dvc, erepo_dir, mocker):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("foo", "foo content", commit="create foo")

    spy = mocker.spy(base, "fs_download")
    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported")
    assert spy.called

    spy.reset_mock()
    dvc.imp(os.fspath(erepo_dir), "foo", "foo_imported", force=True)
    assert not spy.called
