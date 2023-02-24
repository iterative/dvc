import re

import pytest

from dvc.repo.scm_context import SCMContext
from dvc.scm import Git, NoSCM


def pytest_generate_tests(metafunc):
    if "scm_context" in metafunc.fixturenames:
        metafunc.parametrize("scm_context", ["scm", "no_scm"], indirect=True)


@pytest.fixture
def scm_context(request, mocker):
    spec = Git if getattr(request, "param", "scm") == "scm" else NoSCM
    # we'll test `ignore` and `ignore_remove` in a functional test.
    return SCMContext(
        scm=mocker.MagicMock(
            spec=spec,
            **{
                "ignore_remove.return_value": ".gitignore",
                "ignore.return_value": ".gitignore",
            },
        )
    )


def test_scm_track_file(scm_context):
    scm_context.track_file("foo")
    assert scm_context.files_to_track == {"foo"}
    scm_context.track_file("bar")
    assert scm_context.files_to_track == {"foo", "bar"}


def test_scm_track_changed_files(scm_context):
    scm_context.track_changed_files()
    scm_context.scm.add.assert_not_called()

    scm_context.track_file("foo")
    scm_context.track_changed_files()
    scm_context.scm.add.assert_called_once_with({"foo"})


def test_ignore(scm_context):
    scm_context.ignore("foo")

    scm_context.scm.ignore.assert_called_once_with("foo")
    assert scm_context.files_to_track == {".gitignore"}
    assert scm_context.ignored_paths == ["foo"]


def test_ignore_remove(scm_context):
    scm_context.ignore_remove("foo")
    scm_context.scm.ignore_remove.assert_called_once_with("foo")
    assert scm_context.files_to_track == {".gitignore"}


def test_scm_context_reset_on_exit(scm_context):
    with scm_context:
        scm_context.ignore("foo")
        scm_context.track_file("bar")
    assert not scm_context.files_to_track
    assert not scm_context.ignored_paths


def test_scm_context_autostage_changed_files(scm_context):
    scm_context.autostage = True

    with scm_context:
        scm_context.track_file("foo")
        assert scm_context.files_to_track == {"foo"}

    assert not scm_context.files_to_track
    assert not scm_context.ignored_paths
    scm_context.scm.add.assert_called_once_with({"foo"})


def test_scm_context_clears_ignores_on_error(scm_context):
    class CustomException(Exception):
        pass

    with pytest.raises(CustomException), scm_context():  # noqa: PT012
        scm_context.ignore("foo")
        assert scm_context.ignored_paths == ["foo"]
        raise CustomException

    scm_context.scm.ignore_remove.assert_called_once_with("foo")
    assert scm_context.files_to_track == {".gitignore"}
    assert not scm_context.ignored_paths


@pytest.mark.parametrize("autostage", [True, False])
@pytest.mark.parametrize("quiet", [True, False])
def test_scm_context_on_no_files_to_track(caplog, scm_context, autostage, quiet):
    with scm_context(autostage=autostage, quiet=quiet):
        pass

    scm_context.scm.assert_not_called()
    assert not caplog.text


def test_scm_context_remind_to_track(caplog, scm_context):
    with scm_context() as context:
        context.track_file("foo")
        context.track_file("lorem ipsum")
        assert context.files_to_track == {"foo", "lorem ipsum"}

    if isinstance(scm_context.scm, NoSCM):
        assert not caplog.text
    else:
        assert "To track the changes with git, run:" in caplog.text
        match = re.search(r"git add(?: (('.*?')|(\S+)))*", caplog.text)
        assert match
        assert set(match.groups()) == {"'lorem ipsum'", "foo"}


def test_scm_context_remind_disable(caplog, scm_context):
    with scm_context(quiet=True) as context:
        context.track_file("foo")
        assert context.files_to_track == {"foo"}
    assert not caplog.text

    assert scm_context.quiet is False
    scm_context.quiet = True
    with scm_context() as context:
        context.track_file("foo")
        assert context.files_to_track == {"foo"}
    assert not caplog.text


def test_scm_context_decorator(scm_context, mocker):
    from dvc.repo.scm_context import scm_context as decorator

    repo = mocker.MagicMock(scm_context=scm_context)

    def test_method(repo, *args, **kwargs):
        scm_context.track_file("foo")

    method = mocker.MagicMock(wraps=test_method)
    decorator(method, autostage=True)(repo, "arg", kw=1)
    method.assert_called_once_with(repo, "arg", kw=1)
    scm_context.scm.add.assert_called_once_with({"foo"})
