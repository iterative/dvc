import pytest

from dvc.repo.datasets import parse_url_and_type


@pytest.mark.parametrize(
    "url,expected",
    [
        ("s3://bucket/path", {"type": "url", "url": "s3://bucket/path"}),
        ("gs://bucket/path", {"type": "url", "url": "gs://bucket/path"}),
        ("azure://container/path", {"type": "url", "url": "azure://container/path"}),
        (
            "remote://remote_name/path",
            {"type": "url", "url": "remote://remote_name/path"},
        ),
        ("dvcx://dataset_name", {"type": "dvcx", "url": "dvcx://dataset_name"}),
        (
            "dvc+file:///home/user/repository",
            {
                "type": "dvc",
                "url": "file:///home/user/repository",
            },
        ),
        (
            "dvc://git@github.com:iterative/example-get-started.git",
            {"type": "dvc", "url": "git@github.com:iterative/example-get-started.git"},
        ),
        (
            "dvc+https://github.com/iterative/example-get-started.git",
            {
                "type": "dvc",
                "url": "https://github.com/iterative/example-get-started.git",
            },
        ),
        (
            "dvc+ssh://github.com/iterative/example-get-started.git",
            {
                "type": "dvc",
                "url": "ssh://github.com/iterative/example-get-started.git",
            },
        ),
    ],
)
def test_url_parsing(url, expected):
    assert parse_url_and_type(url) == expected
