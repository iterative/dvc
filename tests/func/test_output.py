import pytest

from dvc.output import OUTS_MAP, _get
from dvc.stage import Stage
from tests import PY39, PYARROW_NOT_AVAILABLE

MARKERS = [
    pytest.mark.skipif(PY39, reason=PYARROW_NOT_AVAILABLE),
    pytest.mark.hdfs,
]

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
    pytest.param("hdfs://example.com/dir/path", "hdfs", marks=MARKERS),
    pytest.param("hdfs://example.com/dir/path", "hdfs", marks=MARKERS),
]


def _get_out(dvc, path):
    return _get(Stage(dvc), path, None, None, None, None)


@pytest.mark.parametrize("url,scheme", TESTS)
def test_scheme(dvc, url, scheme):
    # pylint: disable=unidiomatic-typecheck
    assert type(_get_out(dvc, url)) == OUTS_MAP[scheme]
