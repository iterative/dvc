import pytest

from dvc.cli import parse_args
from tests.utils.asserts import called_once_with_subset


@pytest.mark.parametrize(
    "func, args",
    [
        ("add", ("add", "model.pkl")),
        ("imp", ("import", "https://github.com/org/repo", "model.pkl")),
        ("imp_url", ("import-url", "https://dvc.org/data.xml", "data.xml")),
    ],
    ids=["add", "import", "import-url"],
)
def test_add_annotations(mocker, func, args):
    mocker.patch("dvc.repo.Repo")
    cli_args = parse_args(
        [
            *args,
            "--label",
            "example-get-started",
            "--label",
            "model-registry",
            "--type",
            "model",
            "--desc",
            "description",
            "--meta",
            "key1=value1",
            "--meta",
            "key2=value2",
        ]
    )

    _, *posargs = args
    cmd = cli_args.func(cli_args)

    assert cmd.run() == 0
    assert called_once_with_subset(
        getattr(cmd.repo, func),
        posargs,
        desc="description",
        type="model",
        labels=["example-get-started", "model-registry"],
        meta={"key1": "value1", "key2": "value2"},
    )
