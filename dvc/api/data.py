from contextlib import _GeneratorContextManager as GCM
from contextlib import contextmanager
from typing import Any, Dict, Optional

from funcy import reraise

from dvc.exceptions import FileMissingError, OutputNotFoundError, PathMissingError
from dvc.repo import Repo


@contextmanager
def _wrap_exceptions(repo, url):
    from dvc.config import NoRemoteError
    from dvc.exceptions import NoOutputInExternalRepoError, NoRemoteInExternalRepoError

    try:
        yield
    except NoRemoteError as exc:
        raise NoRemoteInExternalRepoError(url) from exc
    except OutputNotFoundError as exc:
        if exc.repo is repo:
            raise NoOutputInExternalRepoError(exc.output, repo.root_dir, url) from exc
        raise
    except FileMissingError as exc:
        raise PathMissingError(exc.path, url) from exc


def get_url(path, repo=None, rev=None, remote=None):
    """
    Returns the URL to the storage location of a data file or directory tracked
    in a DVC repo. For Git repos, HEAD is used unless a rev argument is
    supplied. The default remote is tried unless a remote argument is supplied.

    Raises OutputNotFoundError if the file is not tracked by DVC.

    NOTE: This function does not check for the actual existence of the file or
    directory in the remote storage.
    """
    from dvc.config import NoRemoteError
    from dvc_data.index import StorageKeyError

    repo_kwargs: Dict[str, Any] = {}
    if remote:
        repo_kwargs["config"] = {"core": {"remote": remote}}
    with Repo.open(
        repo, rev=rev, subrepos=True, uninitialized=True, **repo_kwargs
    ) as _repo:
        index, entry = _repo.get_data_index_entry(path)
        with reraise(
            (StorageKeyError, ValueError),
            NoRemoteError(f"no remote specified in {_repo}"),
        ):
            remote_fs, remote_path = index.storage_map.get_remote(entry)
            return remote_fs.unstrip_protocol(remote_path)


class _OpenContextManager(GCM):
    def __init__(self, func, args, kwds):  # pylint: disable=super-init-not-called
        self.gen = func(*args, **kwds)
        self.func, self.args, self.kwds = (  # type: ignore[assignment]
            func,
            args,
            kwds,
        )

    def __getattr__(self, name):
        raise AttributeError("dvc.api.open() should be used in a with statement.")


def open(  # noqa: A001, pylint: disable=redefined-builtin
    path: str,
    repo: Optional[str] = None,
    rev: Optional[str] = None,
    remote: Optional[str] = None,
    mode: str = "r",
    encoding: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    remote_config: Optional[Dict[str, Any]] = None,
):
    """
    Opens a file tracked in a DVC project.

    This function may only be used as a context manager (using the `with`
    keyword, as shown in the examples).

    This function makes a direct connection to the remote storage, so the file
    contents can be streamed. Your code can process the data buffer as it's
    streamed, which optimizes memory usage.

    Note:
        Use dvc.api.read() to load the complete file contents
        in a single function call, no context manager involved.
        Neither function utilizes disc space.

    Args:
        path (str): location and file name of the target to open,
        relative to the root of `repo`.
        repo (str, optional): location of the DVC project or Git Repo.
            Defaults to the current DVC project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
        rev (str, optional): Any `Git revision`_ such as a branch or tag name,
            a commit hash or a dvc experiment name.
            Defaults to HEAD.
            If `repo` is not a Git repo, this option is ignored.
        remote (str, optional): Name of the `DVC remote`_ used to form the
            returned URL string.
            Defaults to the `default remote`_ of `repo`.
            For local projects, the cache is tried before the default remote.
        mode (str, optional): Specifies the mode in which the file is opened.
            Defaults to "r" (read).
            Mirrors the namesake parameter in builtin `open()`_.
            Only reading `mode` is supported.
        encoding(str, optional): `Codec`_ used to decode the file contents.
            Defaults to None.
            This should only be used in text mode.
            Mirrors the namesake parameter in builtin `open()`_.
        config(dict, optional): config to be passed to the DVC repository.
            Defaults to None.
        remote_config(dict, optional): remote config to be passed to the DVC
            repository.
            Defaults to None.

    Returns:
        _OpenContextManager: A context manager that generatse a corresponding
            `file object`_.
            The exact type of file object depends on the mode used.
            For more details, please refer to Python's `open()`_ built-in,
            which is used under the hood.

    Raises:
        AttributeError: If this method is not used as a context manager.
        ValueError: If non-read `mode` is used.

    Examples:

        - Use data or models from a DVC repository.

        Any file tracked in a DVC project (and stored remotely) can be
        processed directly in your Python code with this API.
        For example, an XML file tracked in a public DVC repo on GitHub can be
        processed like this:

        >>> from xml.sax import parse
        >>> import dvc.api
        >>> from mymodule import mySAXHandler

        >>> with dvc.api.open(
        ...     'get-started/data.xml',
        ...     repo='https://github.com/iterative/dataset-registry'
        ... ) as fd:
        ...     parse(fd, mySAXHandler)

        We use a SAX XML parser here because dvc.api.open() is able to stream
        the data from remote storage.
        The mySAXHandler object should handle the event-driven parsing of the
        document in this case.
        This increases the performance of the code (minimizing memory usage),
        and is typically faster than loading the whole data into memory.

        - Accessing private repos

        This is just a matter of using the right repo argument, for example an
        SSH URL (requires that the credentials are configured locally):

        >>> import dvc.api

        >>> with dvc.api.open(
        ...     'features.dat',
        ...     repo='git@server.com:path/to/repo.git'
        ... ) as fd:
        ...     # ... Process 'features'
        ...     pass

        - Use different versions of data

        Any git revision (see `rev`) can be accessed programmatically.
        For example, if your DVC repo has tagged releases of a CSV dataset:

        >>> import csv
        >>> import dvc.api
        >>> with dvc.api.open(
        ...     'clean.csv',
        ...     rev='v1.1.0'
        ... ) as fd:
        ...     reader = csv.reader(fd)
        ...     # ... Process 'clean' data from version 1.1.0

    .. _Git revision:
        https://git-scm.com/docs/revisions

    .. _DVC remote:
        https://dvc.org/doc/command-reference/remote

    .. _default remote:
        https://dvc.org/doc/command-reference/remote/default

    .. _open():
        https://docs.python.org/3/library/functions.html#open

    .. _Codec:
        https://docs.python.org/3/library/codecs.html#standard-encodings

    .. _file object:
        https://docs.python.org/3/glossary.html#term-file-object

    """
    if "r" not in mode:
        raise ValueError("Only reading `mode` is supported.")

    args = (path,)
    kwargs = {
        "repo": repo,
        "remote": remote,
        "rev": rev,
        "mode": mode,
        "encoding": encoding,
        "config": config,
        "remote_config": remote_config,
    }
    return _OpenContextManager(_open, args, kwargs)


def _open(
    path,
    repo=None,
    rev=None,
    remote=None,
    mode="r",
    encoding=None,
    config=None,
    remote_config=None,
):
    repo_kwargs: Dict[str, Any] = {
        "subrepos": True,
        "uninitialized": True,
        "remote": remote,
        "config": config,
        "remote_config": remote_config,
    }

    with Repo.open(repo, rev=rev, **repo_kwargs) as _repo:
        with _wrap_exceptions(_repo, path):
            import os
            from typing import TYPE_CHECKING, Union

            from dvc.exceptions import IsADirectoryError as DvcIsADirectoryError
            from dvc.fs.data import DataFileSystem
            from dvc.fs.dvc import DVCFileSystem

            if TYPE_CHECKING:
                from dvc.fs import FileSystem

            fs: Union["FileSystem", DataFileSystem, DVCFileSystem]
            if os.path.isabs(path):
                fs = DataFileSystem(index=_repo.index.data["local"])
                fs_path = path
            else:
                fs = DVCFileSystem(repo=_repo, subrepos=True)
                fs_path = fs.from_os_path(path)

            try:
                with fs.open(
                    fs_path,
                    mode=mode,
                    encoding=encoding,
                ) as fobj:
                    yield fobj
            except FileNotFoundError as exc:
                raise FileMissingError(path) from exc
            except IsADirectoryError as exc:
                raise DvcIsADirectoryError(f"'{path}' is a directory") from exc


def read(
    path,
    repo=None,
    rev=None,
    remote=None,
    mode="r",
    encoding=None,
    config=None,
    remote_config=None,
):
    """
    Returns the contents of a tracked file (by DVC or Git). For Git repos, HEAD
    is used unless a rev argument is supplied. The default remote is tried
    unless a remote argument is supplied.
    """
    with open(
        path,
        repo=repo,
        rev=rev,
        remote=remote,
        mode=mode,
        encoding=encoding,
        config=config,
        remote_config=remote_config,
    ) as fd:
        return fd.read()
