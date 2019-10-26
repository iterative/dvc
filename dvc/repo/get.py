import logging
import os
import shortuuid

from dvc.stage import Stage
from dvc.config import Config
from dvc.exceptions import (
    NotDvcRepoError,
    UrlNotDvcRepoError,
    GetDVCFileError,
    OutputNotFoundError,
)
from dvc.path_info import PathInfo
from dvc.external_repo import external_repo
from dvc.state import StateNoop
from dvc.utils import remove, resolve_output

logger = logging.getLogger(__name__)


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
        with external_repo(cache_dir=tmp_dir, url=url, rev=rev) as repo:
            # Note: we need to replace state, because in case of getting DVC
            # dependency on CIFS or NFS filesystems, sqlite-based state
            # will be unable to obtain lock
            repo.state = StateNoop()

            # Try any links possible to avoid data duplication.
            #
            # Not using symlink, because we need to remove cache after we are
            # done, and to make that work we would have to copy data over
            # anyway before removing the cache, so we might just copy it
            # right away.
            #
            # Also, we can't use theoretical "move" link type here, because
            # the same cache file might be used a few times in a directory.
            repo.config.set(
                Config.SECTION_CACHE,
                Config.SECTION_CACHE_TYPE,
                "reflink,hardlink,copy",
            )

            o = repo.find_out_by_relpath(path)
            with repo.state:
                repo.cloud.pull(o.get_used_cache())
            o.path_info = PathInfo(os.path.abspath(out))
            with o.repo.state:
                o.checkout()

    except NotDvcRepoError:
        raise UrlNotDvcRepoError(url)
    except OutputNotFoundError:
        raise OutputNotFoundError(path)
    finally:
        remove(tmp_dir)
