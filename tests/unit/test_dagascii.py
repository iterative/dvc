from dvc import dagascii
from dvc.env import DVC_PAGER


def test_find_pager_uses_default_pager_when_found(mocker):
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("os.system", return_value=0)
    m_make_pager = mocker.patch.object(dagascii, "make_pager")

    dagascii.find_pager()

    m_make_pager.assert_called_once_with(dagascii.DEFAULT_PAGER_FORMATTED)


def test_find_pager_returns_plain_pager_when_default_missing(mocker):
    mocker.patch("sys.stdout.isatty", return_value=True)
    mocker.patch("os.system", return_value=1)

    pager = dagascii.find_pager()

    assert pager.__name__ == "plainpager"


def test_find_pager_uses_custom_pager_when_env_var_is_defined(
    mocker, monkeypatch
):
    mocker.patch("sys.stdout.isatty", return_value=True)
    m_make_pager = mocker.patch.object(dagascii, "make_pager")
    monkeypatch.setenv(DVC_PAGER, dagascii.DEFAULT_PAGER)

    dagascii.find_pager()

    m_make_pager.assert_called_once_with(dagascii.DEFAULT_PAGER)


def test_find_pager_returns_plain_pager_when_is_not_atty(mocker):
    mocker.patch("sys.stdout.isatty", return_value=False)

    pager = dagascii.find_pager()

    assert pager.__name__ == "plainpager"
