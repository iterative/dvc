import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest
from attrs import define, evolve, field, has

from dvc.dependency.base import Dependency
from dvc.exceptions import ReproductionError
from dvc.repo.datasets import (
    DatasetNotFoundError,
    DatasetSpec,
    DVCDataset,
    DVCDatasetLock,
    DVCDatasetSpec,
    DVCXDataset,
    DVCXDatasetLock,
    FileInfo,
    URLDataset,
    URLDatasetLock,
)
from dvc_data.hashfile.meta import Meta
from dvc_data.index import HashInfo, Tree

if TYPE_CHECKING:
    from dvc.repo import Repo


@define
class MockedDVCXVersionInfo:
    version: int
    created_at: datetime = field(factory=lambda: datetime.now(timezone.utc))


def evolve_recursive(inst, **changes):
    """Recursive attr.evolve() method, where any attr-based attributes
    will be evolved too.
    """
    for key, value in changes.items():
        v = getattr(inst, key)
        if has(type(v)) and isinstance(value, dict):
            value = evolve_recursive(v, **value)
        changes[key] = value
    return evolve(inst, **changes)


def test_dvc(tmp_dir, scm, dvc: "Repo"):
    datasets = dvc.datasets

    tmp_dir.scm_gen("file", "file", commit="add file")
    dataset = datasets.add("mydataset", tmp_dir.fs_path, "dvc", path="file")
    expected = DVCDataset(
        manifest_path=(tmp_dir / "dvc.yaml").fs_path,
        spec=DVCDatasetSpec(
            name="mydataset", url=tmp_dir.fs_path, type="dvc", path="file"
        ),
        lock=DVCDatasetLock(
            name="mydataset",
            url=tmp_dir.fs_path,
            type="dvc",
            path="file",
            rev_lock=scm.get_rev(),
        ),
    )
    assert "mydataset" in datasets
    assert dataset == datasets["mydataset"] == expected
    tmp_dir.scm_gen("file", "file", commit="update file")

    old, new = datasets.update("mydataset")
    assert old == dataset
    assert old != new
    expected = evolve_recursive(expected, lock={"rev_lock": scm.get_rev()})
    assert new == datasets["mydataset"] == expected

    # noop
    old, new = datasets.update("mydataset")
    assert old == new


def test_dvcx(tmp_dir, dvc, mocker):
    datasets = dvc.datasets

    version_info = [MockedDVCXVersionInfo(1), MockedDVCXVersionInfo(2)]
    version_info.append(version_info[1])
    mocker.patch("dvc.repo.datasets._get_dataset_info", side_effect=version_info)

    dataset = datasets.add("mydataset", "dataset", "dvcx")
    expected = DVCXDataset(
        manifest_path=(tmp_dir / "dvc.yaml").fs_path,
        spec=DatasetSpec(name="mydataset", url="dataset", type="dvcx"),
        lock=DVCXDatasetLock(
            name="mydataset",
            url="dataset",
            type="dvcx",
            version=1,
            created_at=version_info[0].created_at,
        ),
    )
    assert "mydataset" in datasets
    assert dataset == datasets["mydataset"] == expected

    old, new = datasets.update("mydataset")
    assert old == dataset
    assert old != new
    expected = evolve_recursive(
        expected, lock={"version": 2, "created_at": version_info[1].created_at}
    )
    assert new == datasets["mydataset"] == expected

    # noop
    old, new = datasets.update("mydataset")
    assert old == new


def test_url(tmp_dir, dvc, mocker):
    datasets = dvc.datasets

    tree = Tree()
    tree.add(("foo",), Meta(version_id="1"), None)
    tree.digest(with_meta=True)
    tree_meta = Meta(isdir=True)

    def mocked_save(d):
        d.meta, d.obj, d.hash_info = tree_meta, tree, HashInfo("md5", "value.dir")

    mocker.patch.object(Dependency, "save", mocked_save)

    dataset = datasets.add("mydataset", "s3://dataset", "url")
    expected = URLDataset(
        manifest_path=(tmp_dir / "dvc.yaml").fs_path,
        spec=DatasetSpec(name="mydataset", url="s3://dataset", type="url"),
        lock=URLDatasetLock(
            name="mydataset",
            url="s3://dataset",
            type="url",
            meta=Meta(isdir=True),
            files=[FileInfo(relpath="foo", meta=Meta(version_id="1"))],
        ),
    )
    assert "mydataset" in datasets
    assert dataset == datasets["mydataset"] == expected

    tree.add(("bar",), Meta(version_id="2"), None)
    old, new = datasets.update("mydataset")
    assert old == dataset
    assert old != new

    assert expected.lock
    new_files = [
        *expected.lock.files,
        FileInfo(relpath="bar", meta=Meta(version_id="2")),
    ]

    expected = evolve_recursive(expected, lock={"files": new_files})
    assert new == datasets["mydataset"] == expected

    # noop
    old, new = datasets.update("mydataset")
    assert old == new


def test_dvc_dump(tmp_dir, dvc):
    manifest_path = os.path.join(tmp_dir, "dvc.yaml")
    spec = DVCDatasetSpec(
        name="mydataset", url=tmp_dir.fs_path, type="dvc", path="path", rev="main"
    )
    lock = DVCDatasetLock(rev_lock="0" * 40, **spec.to_dict())
    dataset = DVCDataset(manifest_path=manifest_path, spec=spec, lock=lock)

    dvc.datasets.dump(dataset)

    spec_d = {
        "name": "mydataset",
        "type": "dvc",
        "url": tmp_dir.fs_path,
        "path": "path",
        "rev": "main",
    }
    assert (tmp_dir / "dvc.yaml").parse() == {"datasets": [spec_d]}
    assert (tmp_dir / "dvc.lock").parse() == {
        "schema": "2.0",
        "stages": {},
        "datasets": [{**spec_d, "rev_lock": "0" * 40}],
    }

    dvc._reset()
    assert "_datasets" not in vars(dvc.datasets)
    # test that we can read them back
    assert dvc.datasets["mydataset"] == dataset


def test_dvcx_dump(tmp_dir, dvc):
    manifest_path = os.path.join(tmp_dir, "dvc.yaml")
    spec = DatasetSpec(name="mydataset", url="dataset", type="dvcx")
    dt = datetime.now(tz=timezone.utc)
    lock = DVCXDatasetLock(version=1, created_at=dt, **spec.to_dict())
    dataset = DVCXDataset(manifest_path=manifest_path, spec=spec, lock=lock)

    dvc.datasets.dump(dataset)

    spec_d = {"name": "mydataset", "type": "dvcx", "url": "dataset"}
    assert (tmp_dir / "dvc.yaml").parse() == {"datasets": [spec_d]}
    assert (tmp_dir / "dvc.lock").parse() == {
        "schema": "2.0",
        "stages": {},
        "datasets": [{**spec_d, "version": 1, "created_at": dt.isoformat()}],
    }

    dvc._reset()
    assert "_datasets" not in vars(dvc.datasets)
    # test that we can read them back
    assert dvc.datasets["mydataset"] == dataset


def test_url_dump(tmp_dir, dvc):
    manifest_path = os.path.join(tmp_dir, "dvc.yaml")
    spec = DatasetSpec(name="mydataset", url="s3://dataset", type="url")
    files = [FileInfo(relpath="foo", meta=Meta(version_id="1"))]
    lock = URLDatasetLock(meta=Meta(isdir=True), files=files, **spec.to_dict())
    dataset = URLDataset(manifest_path=manifest_path, spec=spec, lock=lock)

    dvc.datasets.dump(dataset)

    spec_d = {"name": "mydataset", "url": "s3://dataset", "type": "url"}
    assert (tmp_dir / "dvc.yaml").parse() == {"datasets": [spec_d]}
    assert (tmp_dir / "dvc.lock").parse() == {
        "schema": "2.0",
        "datasets": [
            {
                **spec_d,
                "meta": {"isdir": True},
                "files": [{"relpath": "foo", "meta": {"version_id": "1"}}],
            }
        ],
        "stages": {},
    }

    dvc._reset()
    assert "_datasets" not in vars(dvc.datasets)
    # test that we can read them back
    assert dvc.datasets["mydataset"] == dataset


def test_invalidation(tmp_dir, dvc):
    manifest_path = os.path.join(tmp_dir, "dvc.yaml")
    spec = DatasetSpec(name="mydataset", url="url1", type="url")
    lock = DVCXDatasetLock(
        name="mydataset",
        url="dataset",
        type="dvcx",
        version=1,
        created_at=datetime.now(tz=timezone.utc),
    )
    dvc.datasets._dump_spec(manifest_path, spec)
    dvc.datasets._dump_lock(manifest_path, lock)

    assert dvc.datasets["mydataset"] == URLDataset(
        manifest_path=manifest_path,
        spec=spec,
        lock=None,  # lock should be discarded
    )


def test_dvc_dataset_pipeline(tmp_dir, dvc, scm):
    dvc.datasets.add("mydataset", tmp_dir.fs_path, "dvc")

    stage = dvc.stage.add(cmd="echo", name="train", deps=["ds://mydataset"])
    assert (tmp_dir / "dvc.yaml").parse() == {
        "datasets": [{"name": "mydataset", "url": tmp_dir.fs_path, "type": "dvc"}],
        "stages": {"train": {"cmd": "echo", "deps": ["ds://mydataset"]}},
    }

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "new"}}]}
    assert dvc.reproduce() == [stage]

    d = (tmp_dir / "dvc.lock").parse()
    assert d["stages"]["train"]["deps"][0] == {
        "path": "ds://mydataset",
        "dataset": d["datasets"][0],
    }

    assert dvc.status() == {}
    assert dvc.reproduce() == []

    tmp_dir.scm_gen("foo", "foo", commit="add foo")
    dvc.datasets.update("mydataset")

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "modified"}}]}
    assert dvc.reproduce() == [stage]


def test_dvcx_dataset_pipeline(mocker, tmp_dir, dvc):
    version_info = [MockedDVCXVersionInfo(1), MockedDVCXVersionInfo(2)]
    mocker.patch("dvc.repo.datasets._get_dataset_info", side_effect=version_info)

    dvc.datasets.add("mydataset", "dataset", "dvcx")

    stage = dvc.stage.add(cmd="echo", name="train", deps=["ds://mydataset"])
    assert (tmp_dir / "dvc.yaml").parse() == {
        "datasets": [{"name": "mydataset", "url": "dataset", "type": "dvcx"}],
        "stages": {"train": {"cmd": "echo", "deps": ["ds://mydataset"]}},
    }

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "new"}}]}
    assert dvc.reproduce() == [stage]

    d = (tmp_dir / "dvc.lock").parse()
    assert d["stages"]["train"]["deps"][0] == {
        "path": "ds://mydataset",
        "dataset": d["datasets"][0],
    }

    assert dvc.status() == {}
    assert dvc.reproduce() == []

    dvc.datasets.update("mydataset")

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "modified"}}]}
    assert dvc.reproduce() == [stage]


def test_url_dataset_pipeline(mocker, tmp_dir, dvc):
    tree = Tree()
    tree.add(("foo",), Meta(version_id="1"), None)
    tree.digest(with_meta=True)
    tree_meta = Meta(isdir=True)

    def mocked_save(d):
        d.meta, d.obj, d.hash_info = tree_meta, tree, HashInfo("md5", "value.dir")

    mocker.patch.object(Dependency, "save", mocked_save)

    dvc.datasets.add("mydataset", "s3://mydataset", "url")

    stage = dvc.stage.add(cmd="echo", name="train", deps=["ds://mydataset"])
    assert (tmp_dir / "dvc.yaml").parse() == {
        "datasets": [{"name": "mydataset", "url": "s3://mydataset", "type": "url"}],
        "stages": {"train": {"cmd": "echo", "deps": ["ds://mydataset"]}},
    }

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "new"}}]}
    assert dvc.reproduce() == [stage]

    d = (tmp_dir / "dvc.lock").parse()
    assert d["stages"]["train"]["deps"][0] == {
        "path": "ds://mydataset",
        "dataset": d["datasets"][0],
    }

    assert dvc.status() == {}
    assert dvc.reproduce() == []

    tree.add(("bar",), Meta(version_id="2"), None)
    dvc.datasets.update("mydataset")

    assert dvc.status() == {"train": [{"changed deps": {"ds://mydataset": "modified"}}]}
    assert dvc.reproduce() == [stage]


def test_pipeline_when_not_in_sync(tmp_dir, dvc):
    manifest_path = os.path.join(tmp_dir, "dvc.yaml")
    spec = DatasetSpec(name="mydataset", url="url1", type="url")
    lock = DVCXDatasetLock(
        name="mydataset",
        url="dataset",
        type="dvcx",
        version=1,
        created_at=datetime.now(tz=timezone.utc),
    )
    dvc.datasets._dump_spec(manifest_path, spec)
    dvc.datasets._dump_lock(manifest_path, lock)

    dvc.stage.add(name="train", cmd="echo", deps=["ds://mydataset"])
    assert dvc.status() == {
        "train": [{"changed deps": {"ds://mydataset": "not in sync"}}]
    }
    with pytest.raises(ReproductionError) as exc:
        dvc.reproduce()
    assert "not in sync" in str(exc.value.__cause__)


def test_collect(tmp_dir, dvc):
    manifest_path1 = os.path.join(tmp_dir, "dvc.yaml")
    dt = datetime.now(tz=timezone.utc)
    spec = DatasetSpec(name="mydataset1", url="url1", type="dvcx")
    lock = DVCXDatasetLock(version=1, created_at=dt, **spec.to_dict())
    mydataset1 = DVCXDataset(manifest_path=manifest_path1, spec=spec, lock=lock)
    dvc.datasets.dump(mydataset1)

    (tmp_dir / "sub").mkdir()
    manifest_path2 = os.path.join(tmp_dir, "sub", "dvc.yaml")
    spec = DVCDatasetSpec(
        name="mydataset2", url=tmp_dir.fs_path, type="dvc", path="path"
    )
    lock = DVCDatasetLock(rev_lock="0" * 40, **spec.to_dict())
    mydataset2 = DVCDataset(manifest_path=manifest_path2, spec=spec, lock=lock)
    dvc.datasets.dump(mydataset2)

    dvc._reset()
    assert "_datasets" not in vars(dvc.datasets)

    assert len(dvc.datasets) == 2
    assert "mydataset1" in dvc.datasets
    assert "mydataset2" in dvc.datasets
    assert list(iter(dvc.datasets)) == ["mydataset1", "mydataset2"]
    assert dvc.datasets["mydataset1"] == mydataset1
    assert dvc.datasets["mydataset2"] == mydataset2
    assert dict(dvc.datasets.items()) == {
        "mydataset1": mydataset1,
        "mydataset2": mydataset2,
    }

    with pytest.raises(DatasetNotFoundError, match=r"^dataset not found$"):
        dvc.datasets["not-existing"]


def test_parametrized(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(
        {
            "datasets": [
                {"name": "${ds1.name}", "url": "${ds1.url}", "type": "dvcx"},
                {
                    "name": "${ds2.name}",
                    "url": "${ds2.url}",
                    "type": "dvc",
                    "path": "${ds2.path}",
                },
                {
                    "name": "${ds3.name}",
                    "url": "${ds3.url}",
                    "type": "url",
                },
            ]
        }
    )
    (tmp_dir / "params.yaml").dump(
        {
            "ds1": {"name": "dogs", "url": "dvcx://dogs"},
            "ds2": {
                "name": "example-get-started",
                "url": "git@github.com:iterative/example-get-started.git",
                "path": "path",
            },
            "ds3": {
                "name": "cloud-versioning-demo",
                "url": "s3://cloud-versioning-demo",
            },
        }
    )

    path = (tmp_dir / "dvc.yaml").fs_path
    assert dict(dvc.datasets.items()) == {
        "dogs": DVCXDataset(
            manifest_path=path,
            spec=DatasetSpec(name="dogs", url="dvcx://dogs", type="dvcx"),
        ),
        "example-get-started": DVCDataset(
            manifest_path=path,
            spec=DVCDatasetSpec(
                name="example-get-started",
                url="git@github.com:iterative/example-get-started.git",
                path="path",
                type="dvc",
            ),
        ),
        "cloud-versioning-demo": URLDataset(
            manifest_path=path,
            spec=DatasetSpec(
                name="cloud-versioning-demo",
                url="s3://cloud-versioning-demo",
                type="url",
            ),
        ),
    }
