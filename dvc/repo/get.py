import logging
import os

import shortuuid

from dvc.exceptions import (
    DvcException,
    NotDvcRepoError,
    OutputNotFoundError,
    PathMissingError,
)
from dvc.external_repo import (
    external_repo,
    cached_clone,
    NoOutputInExternalRepoError,
)
from dvc.path_info import PathInfo
from dvc.stage import Stage
from dvc.utils import resolve_output
from dvc.utils.fs import remove
from dvc.utils.fs import fs_copy

logger = logging.getLogger(__name__)


class GetDVCFileError(DvcException):
    def __init__(self):
        super().__init__(
            "the given path is a DVC-file, you must specify a data file "
            "or a directory"
        )


@staticmethod
def get(url, path, out=None, rev=None):
    out = resolve_output(path, out)

    if Stage.is_valid_filename(out):
        raise GetDVCFileError()

    # Creating a directory right beside the output to make sure that they
    # are on the same filesystem, so we could take the advantage of
    # reflink and/or hardlink. Not using tempfile.TemporaryDirectory
    # because it will create a symlink to tmpfs, which defeats the purpose
    # and won't work with reflink/hardlink.
    dpath = os.path.dirname(os.path.abspath(out))
    tmp_dir = os.path.join(dpath, "." + str(shortuuid.uuid()))
    try:
        try:
            with external_repo(cache_dir=tmp_dir, url=url, rev=rev) as repo:
                # Try any links possible to avoid data duplication.
                #
                # Not using symlink, because we need to remove cache after we
                # are done, and to make that work we would have to copy data
                # over anyway before removing the cache, so we might just copy
                # it right away.
                #
                # Also, we can't use theoretical "move" link type here, because
                # the same cache file might be used a few times in a directory.
                repo.cache.local.cache_types = ["reflink", "hardlink", "copy"]
                output = repo.find_out_by_relpath(path)
                if output.use_cache:
                    _get_cached(repo, output, out)
                    return
                # Non-cached output, fall through and try to copy from git.
        except (NotDvcRepoError, NoOutputInExternalRepoError):
            # Not a DVC repository or, possibly, path is not tracked by DVC.
            # Fall through and try to copy from git.
            pass

        if os.path.isabs(path):
            raise FileNotFoundError

        repo_dir = cached_clone(url, rev=rev)

        fs_copy(os.path.join(repo_dir, path), out)
    except (OutputNotFoundError, FileNotFoundError):
        raise PathMissingError(path, url)
    finally:
        remove(tmp_dir)


def _get_cached(repo, output, out):
    with repo.state:
        repo.cloud.pull(output.get_used_cache())
        output.path_info = PathInfo(os.path.abspath(out))
        failed = output.checkout()
        # This might happen when pull haven't really pulled all the files
        if failed:
            raise FileNotFoundError
