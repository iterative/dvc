import pytest

from dvc.output import _get
from dvc.output import OUTS_MAP
from dvc.stage import Stage
from tests.remotes import HDFS

TESTS = [
    ("s3://bucket/path", "s3"),
    ("gs://bucket/path", "gs"),
    ("ssh://example.com:/dir/path", "ssh"),
    ("path/to/file", "local"),
    ("path\\to\\file", "local"),
    ("file", "local"),
    ("./file", "local"),
    (".\\file", "local"),
    ("../file", "local"),
    ("..\\file", "local"),
    ("unknown://path", "local"),
]

if HDFS.should_test():
    TESTS.append(("hdfs://example.com/dir/path", "hdfs"),)


def _get_out(dvc, path):
    return _get(Stage(dvc), path, None, None, None, None)


@pytest.mark.parametrize("url,scheme", TESTS)
def test_scheme(dvc, url, scheme):
    assert type(_get_out(dvc, url)) == OUTS_MAP[scheme]
