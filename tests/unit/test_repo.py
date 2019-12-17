import os


def test_is_dvc_internal(dvc):
    assert dvc.is_dvc_internal(os.path.join("path", "to", ".dvc", "file"))
    assert not dvc.is_dvc_internal(os.path.join("path", "to-non-.dvc", "file"))
