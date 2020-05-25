import pytest

from dvc.repo.plots.diff import _revisions


@pytest.mark.parametrize(
    "arg_revisions,is_dirty,expected_revisions",
    [
        ([], False, ["working tree"]),
        ([], True, ["HEAD", "working tree"]),
        (["v1", "v2", "working tree"], False, ["v1", "v2", "working tree"]),
        (["v1", "v2", "working tree"], True, ["v1", "v2", "working tree"]),
    ],
)
def test_revisions(mocker, arg_revisions, is_dirty, expected_revisions):
    assert _revisions(arg_revisions, is_dirty) == expected_revisions
