from datetime import datetime, timezone

import pytest

from dvc.cli import main


@pytest.mark.parametrize(
    "spec,lock,expected_output",
    [
        (
            {"name": "ds", "url": "url", "type": "dvc", "path": "path", "rev": "main"},
            {"rev_lock": "0" * 40},
            "Adding ds (url:/path @ main)\n",
        ),
        (
            {"name": "mydataset", "url": "dvcx://dataset", "type": "dvcx"},
            {"version": 1, "created_at": datetime.now(tz=timezone.utc)},
            "Adding mydataset (dvcx://dataset @ v1)\n",
        ),
        (
            {"name": "mydataset", "url": "s3://bucket/path", "type": "url"},
            {
                "files": [{"relpath": "foo", "meta": {"version_id": 1}}],
                "meta": {"isdir": True},
            },
            "Adding mydataset (s3://bucket/path)\n",
        ),
    ],
)
def test_add(dvc, capsys, mocker, spec, lock, expected_output):
    dataset = dvc.datasets._build_dataset("dvc.yaml", spec, spec | lock)

    m = mocker.patch("dvc.repo.datasets.Datasets.add", return_value=dataset)

    assert main(["dataset", "add", spec["name"], f"--{spec['type']}", spec["url"]]) == 0
    out, err = capsys.readouterr()
    assert out == expected_output
    assert not err
    m.assert_called_once()


def test_add_already_exists(dvc, caplog, mocker):
    spec = {"name": "ds", "url": "url", "type": "dvc"}
    dataset = dvc.datasets._build_dataset("dvc.yaml", spec, None)
    mocker.patch("dvc.repo.datasets.Datasets.get", return_value=dataset)

    assert main(["dataset", "add", "ds", "--dvcx", "dataset"]) == 255
    assert "ds already exists in dvc.yaml, use the --force to overwrite" in caplog.text


@pytest.mark.parametrize("lock", ["missing", "unchanged", "updated"])
@pytest.mark.parametrize(
    "spec,old_lock,new_lock,expected_outputs",
    [
        (
            {"name": "ds", "url": "url", "type": "dvc", "path": "path", "rev": "main"},
            {"rev_lock": "0" * 40},
            {"rev_lock": "1" * 40},
            {
                "missing": "Updating ds (url:/path @ main)\n",
                "unchanged": "Nothing to update\n",
                "updated": "Updating ds (000000000 -> 111111111)\n",
            },
        ),
        (
            {"name": "mydataset", "url": "dvcx://dataset", "type": "dvcx"},
            {"version": 1, "created_at": datetime.now(tz=timezone.utc)},
            {"version": 2},
            {
                "missing": "Updating mydataset (dvcx://dataset @ v2)\n",
                "unchanged": "Nothing to update\n",
                "updated": "Updating mydataset (v1 -> v2)\n",
            },
        ),
        (
            {"name": "mydataset", "url": "dvcx://dataset", "type": "dvcx"},
            {"version": 2, "created_at": datetime.now(tz=timezone.utc)},
            {"version": 1},
            {
                "missing": "Updating mydataset (dvcx://dataset @ v1)\n",
                "unchanged": "Nothing to update\n",
                "updated": "Downgrading mydataset (v2 -> v1)\n",
            },
        ),
        (
            {"name": "mydataset", "url": "s3://bucket/path", "type": "url"},
            {
                "files": [
                    {"relpath": "bar", "meta": {"version_id": 2}},
                    {"relpath": "baz", "meta": {"version_id": 3}},
                    {"relpath": "foo", "meta": {"version_id": 1}},
                ],
                "meta": {"isdir": True},
            },
            {
                "files": [
                    {"relpath": "bar", "meta": {"version_id": 2}},  # unchanged
                    {"relpath": "baz", "meta": {"version_id": 4}},  # modified
                    # `foo` deleted
                    {"relpath": "foobar", "meta": {"version_id": 5}},  # new
                ],
                "meta": {"isdir": True},
            },
            {
                "missing": "Updating mydataset (s3://bucket/path)\n",
                "unchanged": "Nothing to update\n",
                "updated": (
                    "Updating mydataset (s3://bucket/path)\n"
                    "M\tbaz\n"
                    "A\tfoobar\n"
                    "D\tfoo\n"
                ).expandtabs(8),
            },
        ),
    ],
)
def test_update(dvc, capsys, mocker, spec, old_lock, new_lock, expected_outputs, lock):
    if lock == "missing":
        new_lock = spec | old_lock | new_lock
        old_lock = None
    elif lock == "unchanged":
        old_lock = new_lock = spec | old_lock
    else:
        old_lock = spec | old_lock
        new_lock = old_lock | new_lock

    old = dvc.datasets._build_dataset("dvc.yaml", spec, old_lock)
    new = dvc.datasets._build_dataset("dvc.yaml", spec, new_lock)

    m = mocker.patch("dvc.repo.datasets.Datasets.update", return_value=(old, new))
    assert main(["dataset", "update", spec["name"]]) == 0
    out, err = capsys.readouterr()
    assert out == expected_outputs[lock]
    assert not err

    m.assert_called_once()
