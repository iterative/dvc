import os
from collections import Counter
from contextlib import _GeneratorContextManager as GCM
from typing import Dict, Iterable, Optional, Union

from funcy import first, reraise

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
        fs_path = _repo.dvcfs.from_os_path(path)
        with reraise(FileNotFoundError, PathMissingError(path, repo)):
            info = _repo.dvcfs.info(fs_path)

        dvc_info = info.get("dvc_info")
        if not dvc_info:
            raise OutputNotFoundError(path, repo)

        dvc_repo = info["repo"]
        md5 = dvc_info["md5"]

        return dvc_repo.cloud.get_url_for(remote, checksum=md5)


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


def open(  # noqa, pylint: disable=redefined-builtin
    path: str,
    repo: Optional[str] = None,
    rev: Optional[str] = None,
    remote: Optional[str] = None,
    mode: str = "r",
    encoding: Optional[str] = None,
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
    }
    return _OpenContextManager(_open, args, kwargs)


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


def params_show(
    *targets: str,
    repo: Optional[str] = None,
    stages: Optional[Union[str, Iterable[str]]] = None,
    rev: Optional[str] = None,
    deps: bool = False,
) -> Dict:
    """Get parameters tracked in `repo`.

    Without arguments, this function will retrieve all params from all tracked
    parameter files, for the current working tree.

    See the options below to restrict the parameters retrieved.

    Args:
        *targets (str, optional): Names of the parameter files to retrieve
        params from. For example, "params.py, myparams.toml".
        If no `targets` are provided, all parameter files tracked in `dvc.yaml`
        will be used.
        Note that targets don't necessarily have to be defined in `dvc.yaml`.
        repo (str, optional): location of the DVC repository.
            Defaults to the current project (found by walking up from the
            current working directory tree).
            It can be a URL or a file system path.
            Both HTTP and SSH protocols are supported for online Git repos
            (e.g. [user@]server:project.git).
        stages (Union[str, Iterable[str]], optional): Name or names of the
            stages to retrieve parameters from.
            Defaults to `None`.
            If `None`, all parameters from all stages will be retrieved.
        rev (str, optional): Name of the `Git revision`_ to retrieve parameters
            from.
            Defaults to `None`.
            An example of git revision can be a branch or tag name, a commit
            hash or a dvc experiment name.
            If `repo` is not a Git repo, this option is ignored.
            If `None`, the current working tree will be used.
        deps (bool, optional): Whether to retrieve only parameters that are
            stage dependencies or not.
            Defaults to `False`.

    Returns:
        Dict: See Examples below.

    Examples:

        - No arguments.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show()
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "featurize": {
                "max_features": 200,
                "ngrams": 2
            },
            "train": {
                "seed": 20170428,
                "n_est": 50,
                "min_split": 0.01
            }
        }

        ---

        - Filtering with `stages`.

        Working on https://github.com/iterative/example-get-started

        `stages` can a single string:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(stages="prepare")
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            }
        }

        Or an iterable of strings:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(stages=["prepare", "train"])
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "train": {
                "seed": 20170428,
                "n_est": 50,
                "min_split": 0.01
            }
        }

        ---

        - Using `rev`.

        Working on https://github.com/iterative/example-get-started

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(rev="tune-hyperparams")
        >>> print(json.dumps(params, indent=4))
        {
            "prepare": {
                "split": 0.2,
                "seed": 20170428
            },
            "featurize": {
                "max_features": 200,
                "ngrams": 2
            },
            "train": {
                "seed": 20170428,
                "n_est": 100,
                "min_split": 8
            }
        }

        ---

        - Using `targets`.

        Working on `multi-params-files` folder of
        https://github.com/iterative/pipeline-conifguration

        You can pass a single target:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show("params.yaml")
        >>> print(json.dumps(params, indent=4))
        {
            "run_mode": "prod",
            "configs": {
                "dev": "configs/params_dev.yaml",
                "test": "configs/params_test.yaml",
                "prod": "configs/params_prod.yaml"
            },
            "evaluate": {
                "dataset": "micro",
                "size": 5000,
                "metrics": ["f1", "roc-auc"],
                "metrics_file": "reports/metrics.json",
                "plots_cm": "reports/plot_confusion_matrix.png"
            }
        }


        Or multiple targets:

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(
        ...     "configs/params_dev.yaml", "configs/params_prod.yaml")
        >>> print(json.dumps(params, indent=4))
        {
            "configs/params_prod.yaml:run_mode": "prod",
            "configs/params_prod.yaml:config_file": "configs/params_prod.yaml",
            "configs/params_prod.yaml:data_load": {
                "dataset": "large",
                "sampling": {
                "enable": true,
                "size": 50000
                }
            },
            "configs/params_prod.yaml:train": {
                "epochs": 1000
            },
            "configs/params_dev.yaml:run_mode": "dev",
            "configs/params_dev.yaml:config_file": "configs/params_dev.yaml",
            "configs/params_dev.yaml:data_load": {
                "dataset": "development",
                "sampling": {
                "enable": true,
                "size": 1000
                }
            },
            "configs/params_dev.yaml:train": {
                "epochs": 10
            }
        }

        ---

        - Git URL as `repo`.

        >>> import json
        >>> import dvc.api
        >>> params = dvc.api.params_show(
        ...     repo="https://github.com/iterative/demo-fashion-mnist")
        {
            "train": {
                "batch_size": 128,
                "hidden_units": 64,
                "dropout": 0.4,
                "num_epochs": 10,
                "lr": 0.001,
                "conv_activation": "relu"
            }
        }


    .. _Git revision:
        https://git-scm.com/docs/revisions

    """
    if isinstance(stages, str):
        stages = [stages]

    def _onerror_raise(result: Dict, exception: Exception, *args, **kwargs):
        raise exception

    def _postprocess(params):
        processed = {}
        for rev, rev_data in params.items():
            processed[rev] = {}

            counts = Counter()
            for file_data in rev_data["data"].values():
                for k in file_data["data"]:
                    counts[k] += 1

            for file_name, file_data in rev_data["data"].items():
                to_merge = {
                    (k if counts[k] == 1 else f"{file_name}:{k}"): v
                    for k, v in file_data["data"].items()
                }
                processed[rev] = {**processed[rev], **to_merge}

        if "workspace" in processed:
            del processed["workspace"]

        return processed[first(processed)]

    with Repo.open(repo) as _repo:
        params = _repo.params.show(
            revs=rev if rev is None else [rev],
            targets=targets,
            deps=deps,
            onerror=_onerror_raise,
            stages=stages,
        )

    return _postprocess(params)


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
