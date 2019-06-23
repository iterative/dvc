import os
import shortuuid

from dvc.config import Config
from dvc.path_info import PathInfo
from dvc.external_repo import ExternalRepo
from dvc.utils.compat import urlparse
from dvc.utils import remove


@staticmethod
def get(url, path, out=None, rev=None):
    out = out or os.path.basename(urlparse(path).path)

    # Creating a directory right beside the output to make sure that they
    # are on the same filesystem, so we could take the advantage of
    # reflink and/or hardlink. Not using tempfile.TemporaryDirectory
    # because it will create a symlink to tmpfs, which defeats the purpose
    # and won't work with reflink/hardlink.
    dpath = os.path.dirname(os.path.abspath(out))
    tmp_dir = os.path.join(dpath, "." + str(shortuuid.uuid()))
    try:
        erepo = ExternalRepo(tmp_dir, url=url, rev=rev)
        erepo.install()
        # Try any links possible to avoid data duplication.
        #
        # Not using symlink, because we need to remove cache after we are
        # done, and to make that work we would have to copy data over
        # anyway before removing the cache, so we might just copy it
        # right away.
        #
        # Also, we can't use theoretical "move" link type here, because
        # the same cache file might be used a few times in a directory.
        erepo.repo.config.set(
            Config.SECTION_CACHE,
            Config.SECTION_CACHE_TYPE,
            "reflink,hardlink,copy",
        )
        src = os.path.join(erepo.path, urlparse(path).path.lstrip("/"))
        o, = erepo.repo.find_outs_by_path(src)
        erepo.repo.fetch(o.stage.path)
        o.path_info = PathInfo(os.path.abspath(out))
        with o.repo.state:
            o.checkout()
        erepo.repo.scm.git.close()
    finally:
        if os.path.exists(tmp_dir):
            remove(tmp_dir)
