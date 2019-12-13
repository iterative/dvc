from dvc.utils.compat import fspath


def test_is_dvc_internal(tmp_dir, dvc):
    assert dvc.is_dvc_internal(fspath(tmp_dir / ".dvc" / "file"))
    assert not dvc.is_dvc_internal("file")
