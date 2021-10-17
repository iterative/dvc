import os
from contextlib import _GeneratorContextManager as GCM
from contextlib import contextmanager
from typing import ContextManager, Iterator

from dvc.exceptions import NoOutputInExternalRepoError, OutputNotFoundError
from dvc.repo import Repo
from dvc.repo_path import RepoPath


def files(path=os.curdir, repo=None, rev=None) -> ContextManager[RepoPath]:
    @contextmanager
    def inner() -> Iterator["RepoPath"]:
        with Repo.open(
            repo, rev=rev, subrepos=True, uninitialized=True
        ) as root_repo:
            yield RepoPath(path, fs=root_repo.repo_fs)

    return inner()


def get_url(path, repo=None, rev=None, remote=None):
    """
    Returns the URL to the storage location of a data file or directory tracked
    in a DVC repo. For Git repos, HEAD is used unless a rev argument is
    supplied. The default remote is tried unless a remote argument is supplied.

    Raises OutputNotFoundError if the file is not tracked by DVC.

    NOTE: This function does not check for the actual existence of the file or
    directory in the remote storage.
    """
    try:
        with files(path, repo=repo, rev=rev) as path_obj:
            return path_obj.url(remote=remote)
    except NoOutputInExternalRepoError as exc:
        raise OutputNotFoundError(exc.path, repo=repo)


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

    def _open():
        with files(path, repo=repo, rev=rev) as path_obj:
            with path_obj.open(  # pylint: disable=not-context-manager
                remote=remote, mode=mode, encoding=encoding
            ) as fd:
                yield fd

    return _OpenContextManager(_open, (), {})


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


def read(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """
    Returns the contents of a tracked file (by DVC or Git). For Git repos, HEAD
    is used unless a rev argument is supplied. The default remote is tried
    unless a remote argument is supplied.
    """
    with files(path, repo=repo, rev=rev) as path_obj:
        return path_obj.read(remote=remote, mode=mode, encoding=encoding)


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
