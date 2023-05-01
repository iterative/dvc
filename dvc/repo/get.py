import logging
import os
from typing import TYPE_CHECKING, Union

from dvc.dvcfile import PROJECT_FILE
from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.utils import resolve_output
from dvc.utils.fs import remove

if TYPE_CHECKING:
    from dvc.fs.dvc import DVCFileSystem


logger = logging.getLogger(__name__)


class GetDVCFileError(DvcException):
    def __init__(self):
        super().__init__(
            "the given path is a DVC file, you must specify a data file or a directory"
        )


def resolve_artifact_path(path):
    parts = path.split(":")
    if len(parts) == 1:
        dvcyaml = None
    elif len(parts) == 2:
        dvcyaml, path = parts
    else:
        raise InvalidArgumentError("more than one ':' found")  # todo replace error
    if dvcyaml is not None and dvcyaml[-len(PROJECT_FILE) :] != PROJECT_FILE:
        dvcyaml = os.path.join(dvcyaml, PROJECT_FILE)
    return dvcyaml, path


def get(url, path, out=None, rev=None, jobs=None, force=False):
    import shortuuid

    from dvc.dvcfile import is_valid_filename
    from dvc.external_repo import external_repo
    from dvc.fs.callbacks import Callback

    dvcyaml, path = resolve_artifact_path(path)
    out = resolve_output(path, out, force=force)

    if is_valid_filename(out):
        raise GetDVCFileError()

    # Creating a directory right beside the output to make sure that they
    # are on the same filesystem, so we could take the advantage of
    # reflink and/or hardlink. Not using tempfile.TemporaryDirectory
    # because it will create a symlink to tmpfs, which defeats the purpose
    # and won't work with reflink/hardlink.
    dpath = os.path.dirname(os.path.abspath(out))
    tmp_dir = os.path.join(dpath, "." + str(shortuuid.uuid()))

    # Try any links possible to avoid data duplication.
    #
    # Not using symlink, because we need to remove cache after we
    # are done, and to make that work we would have to copy data
    # over anyway before removing the cache, so we might just copy
    # it right away.
    #
    # Also, we can't use theoretical "move" link type here, because
    # the same cache file might be used a few times in a directory.
    cache_types = ["reflink", "hardlink", "copy"]
    try:
        with external_repo(
            url=url,
            rev=rev,
            subrepos=True,
            uninitialized=True,
            cache_dir=tmp_dir,
            cache_types=cache_types,
        ) as repo:
            from dvc.fs.data import DataFileSystem

            if dvcyaml:
                name, path = path, None
                artifacts = repo.artifacts.read()
                if dvcyaml not in artifacts:
                    raise InvalidArgumentError(
                        f"'{dvcyaml}' doesn't exist"
                        " or doesn't have an artifacts section"
                    )
                if name not in artifacts[dvcyaml]:
                    raise InvalidArgumentError(
                        f"Artifact '{name}' doesn't exist in '{dvcyaml}'"
                    )
                path = repo.artifacts.read()[dvcyaml][name].path
                if path is None:
                    raise InvalidArgumentError(
                        "`path` is None for '{name}' in '{dvcyaml}'"
                    )
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
    finally:
        remove(tmp_dir)
