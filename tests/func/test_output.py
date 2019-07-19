import os
import sys

import pytest

from dvc.stage import Stage
from dvc.output import _get, OUTS_MAP


TESTS = [
    ("s3://bucket/path", "s3"),
    ("gs://bucket/path", "gs"),
    ("ssh://example.com:/dir/path", "ssh"),
    pytest.param(
        "hdfs://example.com/dir/path",
        "hdfs",
        marks=pytest.mark.skipif(
            sys.version_info[0] == 2 and os.name == "nt",
            reason="Not supported for python 2 on Windows.",
        ),
    ),
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
def test_scheme(dvc_repo, url, scheme):
    assert type(_get_out(dvc_repo, url)) == OUTS_MAP[scheme]
