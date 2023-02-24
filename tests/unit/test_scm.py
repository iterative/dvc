import pytest

from dvc.exceptions import DvcException
from dvc.scm import resolve_rev


def test_resolve_rev_empty_git_repo(scm):
    with pytest.raises(DvcException, match="unknown Git revision 'HEAD'"):
        resolve_rev(scm, "HEAD")
