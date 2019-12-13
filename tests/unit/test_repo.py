from dvc.system import System
from dvc.utils.compat import fspath


def test_is_dvc_internal(tmp_dir, dvc):
    assert dvc.is_dvc_internal(fspath(tmp_dir / ".dvc" / "file"))
    assert not dvc.is_dvc_internal("file")


def test_destroy(tmp_dir, dvc):
    dvc.config.set("cache", "type", "symlink")

    tmp_dir.dvc_gen('file', 'text')
    tmp_dir.dvc_gen({'dir': {'file': 'lorem', 'subdir/file': 'ipsum'}})

    dvc.destroy()

    # Remove all the files related to DVC
    assert not (tmp_dir / ".dvc").exists()
    assert not (tmp_dir / "file.dvc").exists()
    assert not (tmp_dir / "dir.dvc").exists()

    # Leave the rest of the files
    assert (tmp_dir / "file").is_file()
    assert (tmp_dir / "dir").is_dir()
    assert (tmp_dir / "dir" / "file").is_file()
    assert (tmp_dir / "dir" / "subdir" / "file").is_file()

    # Make sure that data was unprotected after `destroy`
    assert not System.is_symlink(fspath(tmp_dir / "foo"))
    assert not System.is_symlink(fspath(tmp_dir / "file"))
    assert not System.is_symlink(fspath(tmp_dir / "dir"))
    assert not System.is_symlink(fspath(tmp_dir / "dir" / "file"))
    assert not System.is_symlink(fspath(tmp_dir / "dir" / "subdir" / "file"))
