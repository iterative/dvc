import pytest

from dvc.repo.plots.diff import _revisions


@pytest.mark.parametrize(
    "arg_revisions,is_dirty,expected_revisions",
    [
        ([], False, ["workspace"]),
        ([], True, ["HEAD", "workspace"]),
        (["v1", "v2", "workspace"], False, ["v1", "v2", "workspace"]),
        (["v1", "v2", "workspace"], True, ["v1", "v2", "workspace"]),
    ],
)
def test_revisions(mocker, arg_revisions, is_dirty, expected_revisions):
    assert _revisions(arg_revisions, is_dirty) == expected_revisions
