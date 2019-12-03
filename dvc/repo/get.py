import logging
import os
import shutil

import shortuuid

from dvc.exceptions import GetDVCFileError
from dvc.exceptions import NotDvcRepoError
from dvc.exceptions import OutputNotFoundError
from dvc.exceptions import UrlNotDvcRepoError
from dvc.exceptions import NoOutputInExternalRepoError
from dvc.exceptions import FileOutsideRepoError
from dvc.external_repo import external_repo
from dvc.path_info import PathInfo
from dvc.stage import Stage
from dvc.state import StateNoop
from dvc.utils import resolve_output
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


def copy_git_file(repo, src, dst):
    try:
        # Assert that the file does indeed exist in git before attempting to
        # copy it.
        repo.scm.repo.head.commit.tree[src]  # noqa
    except KeyError:
        raise FileOutsideRepoError(src)

    src_full_path = os.path.join(repo.root_dir, src)
    dst_full_path = os.path.abspath(dst)
    if os.path.isdir(src_full_path):
        shutil.copytree(src_full_path, dst_full_path)
    else:
        shutil.copy2(src_full_path, dst_full_path)


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
            repo.cache.local.cache_types = ["reflink", "hardlink", "copy"]

            o = repo.find_out_by_relpath(path)
            with repo.state:
                repo.cloud.pull(o.get_used_cache())
            o.path_info = PathInfo(os.path.abspath(out))
            with o.repo.state:
                o.checkout()

    except NoOutputInExternalRepoError:
        copy_git_file(repo, path, out)
    except NotDvcRepoError:
        raise UrlNotDvcRepoError(url)
    except OutputNotFoundError:
        raise OutputNotFoundError(path)
    finally:
        remove(tmp_dir)
