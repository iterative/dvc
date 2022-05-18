import errno
import hashlib
import json
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from ._ignore import Ignore
    from .fs.base import AnyFSPath, FileSystem


def get_mtime_and_size(
    path: "AnyFSPath", fs: "FileSystem", ignore: "Ignore" = None
) -> Tuple[str, int]:
    import nanotime

    if not fs.isdir(path):
        base_stat = fs.info(path)
        size = base_stat["size"]
        mtime = str(int(nanotime.timestamp(base_stat["mtime"])))
        return mtime, size

    size = 0
    files_mtimes = {}
    if ignore:
        walk_iterator = ignore.find(fs, path)
    else:
        walk_iterator = fs.find(path)
    for file_path in walk_iterator:
        try:
            stats = fs.info(file_path)
        except OSError as exc:
            # NOTE: broken symlink case.
            if exc.errno != errno.ENOENT:
                raise
            continue
        size += stats["size"]
        files_mtimes[file_path] = stats["mtime"]

    # We track file changes and moves, which cannot be detected with simply
    # max(mtime(f) for f in non_ignored_files)
    hasher = hashlib.md5()
    hasher.update(json.dumps(files_mtimes, sort_keys=True).encode("utf-8"))
    mtime = hasher.hexdigest()
    return mtime, size
