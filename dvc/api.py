import os
from contextlib import _GeneratorContextManager as GCM

from funcy import reraise

from dvc.exceptions import OutputNotFoundError, PathMissingError
from dvc.repo import Repo


def get_url(path, repo=None, rev=None, remote=None):
    """
    Returns the URL to the storage location of a data file or directory tracked
    in a DVC repo. For Git repos, HEAD is used unless a rev argument is
    supplied. The default remote is tried unless a remote argument is supplied.

    Raises OutputNotFoundError if the file is not tracked by DVC.

    NOTE: This function does not check for the actual existence of the file or
    directory in the remote storage.
    """
    with Repo.open(repo, rev=rev, subrepos=True, uninitialized=True) as _repo:
        fs_path = _repo.fs.path.join(_repo.root_dir, path)
        with reraise(FileNotFoundError, PathMissingError(path, repo)):
            info = _repo.repo_fs.info(fs_path)

        if not info["isdvc"]:
            raise OutputNotFoundError(path, repo)

        cloud = info["repo"].cloud
        dvc_path = _repo.fs.path.relpath(fs_path, info["repo"].root_dir)

        if not os.path.isabs(path):
            dvc_path = dvc_path.replace("\\", "/")

        md5 = info["repo"].dvcfs.info(dvc_path)["md5"]
        return cloud.get_url_for(remote, checksum=md5)


def open(  # noqa, pylint: disable=redefined-builtin
    path, repo=None, rev=None, remote=None, mode="r", encoding=None
):
    """
    Open file in the supplied path tracked in a repo (both DVC projects and
    plain Git repos are supported). For Git repos, HEAD is used unless a rev
    argument is supplied. The default remote is tried unless a remote argument
    is supplied. It may only be used as a context manager:

        with dvc.api.open(
                'path/to/file',
                repo='https://example.com/url/to/repo'
                ) as fd:
            # ... Handle file object fd
    """
    args = (path,)
    kwargs = {
        "repo": repo,
        "remote": remote,
        "rev": rev,
        "mode": mode,
        "encoding": encoding,
    }
    return _OpenContextManager(_open, args, kwargs)


class _OpenContextManager(GCM):
    def __init__(
        self, func, args, kwds
    ):  # pylint: disable=super-init-not-called
        self.gen = func(*args, **kwds)
        self.func, self.args, self.kwds = func, args, kwds

    def __getattr__(self, name):
        raise AttributeError(
            "dvc.api.open() should be used in a with statement."
        )


def _open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    with Repo.open(repo, rev=rev, subrepos=True, uninitialized=True) as _repo:
        with _repo.open_by_relpath(
            path, remote=remote, mode=mode, encoding=encoding
        ) as fd:
            yield fd


def read(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """
    Returns the contents of a tracked file (by DVC or Git). For Git repos, HEAD
    is used unless a rev argument is supplied. The default remote is tried
    unless a remote argument is supplied.
    """
    with open(
        path, repo=repo, rev=rev, remote=remote, mode=mode, encoding=encoding
    ) as fd:
        return fd.read()


def make_checkpoint():
    """
    Signal DVC to create a checkpoint experiment.

    If the current process is being run from DVC, this function will block
    until DVC has finished creating the checkpoint. Otherwise, this function
    will return immediately.
    """
    import builtins
    from time import sleep

    from dvc.env import DVC_CHECKPOINT, DVC_ROOT
    from dvc.stage.monitor import CheckpointTask

    if os.getenv(DVC_CHECKPOINT) is None:
        return

    root_dir = os.getenv(DVC_ROOT, Repo.find_root())
    signal_file = os.path.join(
        root_dir, Repo.DVC_DIR, "tmp", CheckpointTask.SIGNAL_FILE
    )

    with builtins.open(signal_file, "w", encoding="utf-8") as fobj:
        # NOTE: force flushing/writing empty file to disk, otherwise when
        # run in certain contexts (pytest) file may not actually be written
        fobj.write("")
        fobj.flush()
        os.fsync(fobj.fileno())
    while os.path.exists(signal_file):
        sleep(0.1)
