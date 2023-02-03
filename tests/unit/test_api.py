import pytest

from dvc import api


def test_open_raises_error_if_no_context(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo-text")

    fd = api.open("foo")
    with pytest.raises(AttributeError, match="should be used in a with statement."):
        fd.read()


def test_open_rev_raises_error_on_wrong_mode(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo-text")

    with pytest.raises(ValueError, match="Only reading `mode` is supported."):
        with api.open("foo", mode="w"):
            pass
