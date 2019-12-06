import os

from dvc import dagascii
from dvc.env import DVC_PAGER


def test_less_pager_returned_when_less_found(mocker):
    mocker.patch.object(os, "system", return_value=0)

    pager = dagascii.find_pager()

    assert pager.cmd == dagascii.DEFAULT_PAGER_FORMATTED


def test_plainpager_returned_when_less_missing(mocker):
    mocker.patch.object(os, "system", return_value=1)

    pager = dagascii.find_pager()

    assert pager.__name__ == "plainpager"


def test_tempfilepager_returned_when_var_defined(monkeypatch):
    monkeypatch.setenv(DVC_PAGER, dagascii.DEFAULT_PAGER)

    pager = dagascii.find_pager()

    assert pager.cmd == dagascii.DEFAULT_PAGER
