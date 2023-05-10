import logging
import os
from typing import TYPE_CHECKING, Union

from dvc.exceptions import DvcException
from dvc.utils import resolve_output

if TYPE_CHECKING:
    from dvc.fs.dvc import DVCFileSystem


logger = logging.getLogger(__name__)


class GetDVCFileError(DvcException):
    def __init__(self):
        super().__init__(
            "the given path is a DVC file, you must specify a data file or a directory"
        )


def get(url, path, out=None, rev=None, jobs=None, force=False):
    from dvc.dvcfile import is_valid_filename
    from dvc.fs.callbacks import Callback
    from dvc.repo import Repo

    out = resolve_output(path, out, force=force)

    if is_valid_filename(out):
        raise GetDVCFileError()

    with Repo.open(
        url=url,
        rev=rev,
        subrepos=True,
        uninitialized=True,
    ) as repo:
        from dvc.fs.data import DataFileSystem

        fs: Union[DataFileSystem, "DVCFileSystem"]
        if os.path.isabs(path):
            fs = DataFileSystem(index=repo.index.data["local"])
            fs_path = fs.from_os_path(path)
        else:
            fs = repo.dvcfs
            fs_path = fs.from_os_path(path)

        with Callback.as_tqdm_callback(
            desc=f"Downloading {fs.path.name(path)}",
            unit="files",
        ) as cb:
            fs.get(
                fs_path,
                os.path.abspath(out),
                batch_size=jobs,
                callback=cb,
            )
