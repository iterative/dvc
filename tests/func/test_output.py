import pytest

from dvc.stage import Stage
from dvc.output import _get, OUTS_MAP


TESTS = [
    ("s3://bucket/path", "s3"),
    ("gs://bucket/path", "gs"),
    ("ssh://example.com:/dir/path", "ssh"),
    ("hdfs://example.com/dir/path", "hdfs"),
    ("path/to/file", "local"),
    ("path\\to\\file", "local"),
    ("file", "local"),
    ("./file", "local"),
    (".\\file", "local"),
    ("../file", "local"),
    ("..\\file", "local"),
    ("unknown://path", "local"),
]


def _get_out(dvc, path):
    return _get(Stage(dvc), path, None, None, None, None)


@pytest.mark.parametrize("url,scheme", TESTS)
def test_scheme(dvc, url, scheme):
    assert type(_get_out(dvc, url)) == OUTS_MAP[scheme]
