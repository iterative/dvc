from typing import Literal, TypedDict, Union


class DVCXDataset(TypedDict):
    type: Literal["dvcx"]
    name: str
    version: int


class DVCDataset(TypedDict):
    type: Literal["dvc"]
    url: str
    path: str
    sha: str


class URLDataset(TypedDict):
    type: Literal["url"]
    files: list[str]
    path: str


def get(name: str) -> Union[DVCXDataset, DVCDataset, URLDataset]:
    from difflib import get_close_matches

    from dvc.fs import get_cloud_fs
    from dvc.repo import Repo, datasets

    repo = Repo()
    try:
        dataset = repo.datasets[name]
    except datasets.DatasetNotFoundError as e:
        add_note = getattr(e, "add_note", lambda _: None)
        if matches := get_close_matches(name, repo.datasets):
            add_note(f"Did you mean: {matches[0]!r}?")
        raise

    if dataset._invalidated:
        raise ValueError(f"dataset not in sync. Sync with 'dvc ds update {name}'.")
    if not dataset.lock:
        raise ValueError("missing lock information")
    if dataset.type == "dvc":
        return DVCDataset(
            type="dvc",
            url=dataset.lock.url,
            path=dataset.lock.path,
            sha=dataset.lock.rev_lock,
        )
    if dataset.type == "dvcx":
        return DVCXDataset(
            type="dvcx", name=dataset.name_version[0], version=dataset.lock.version
        )
    if dataset.type == "url":
        fs_cls, _, path = get_cloud_fs(repo.config, url=dataset.lock.url)
        assert fs_cls
        join_version = getattr(fs_cls, "join_version", lambda path, _: path)
        protocol = fs_cls.protocol
        versioned_path = join_version(path, dataset.lock.meta.version_id)
        versioned_path = f"{protocol}://{versioned_path}"
        files = [
            join_version(
                fs_cls.join(versioned_path, file.relpath), file.meta.version_id
            )
            for file in dataset.lock.files
        ]
        return URLDataset(type="url", files=files, path=versioned_path)
