import importlib
import os
import sys
import copy
from contextlib import contextmanager

try:
    from contextlib import _GeneratorContextManager as GCM
except ImportError:
    from contextlib import GeneratorContextManager as GCM

import ruamel.yaml
from voluptuous import Schema, Invalid

from dvc.utils.compat import urlparse, builtin_str, FileNotFoundError
from dvc.repo import Repo
from dvc.external_repo import external_repo


def get_url(path, repo=None, rev=None, remote=None):
    """Returns an url of a resource specified by path in repo"""
    with _make_repo(repo, rev=rev) as _repo:
        abspath = os.path.join(_repo.root_dir, path)
        out, = _repo.find_outs_by_path(abspath)
        remote_obj = _repo.cloud.get_remote(remote)
        return str(remote_obj.checksum_to_path_info(out.checksum))


def open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Opens a specified resource as a file descriptor"""
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
    def __init__(self, func, args, kwds):
        self.gen = func(*args, **kwds)
        self.func, self.args, self.kwds = func, args, kwds

    def __getattr__(self, name):
        raise AttributeError(
            "dvc.api.open() should be used in a with statement"
        )


def _open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    with _make_repo(repo, rev=rev) as _repo:
        abspath = os.path.join(_repo.root_dir, path)
        with _repo.open(
            abspath, remote=remote, mode=mode, encoding=encoding
        ) as fd:
            yield fd


def read(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Read a specified resource into string"""
    with open(
        path, repo=repo, rev=rev, remote=remote, mode=mode, encoding=encoding
    ) as fd:
        return fd.read()


@contextmanager
def _make_repo(repo_url, rev=None):
    if not repo_url or urlparse(repo_url).scheme == "":
        assert rev is None, "Custom revision is not supported for local repo"
        yield Repo(repo_url)
    else:
        with external_repo(url=repo_url, rev=rev) as repo:
            yield repo


def summon(name, fname="dvcsummon.yaml", args=None, repo=None, rev=None):
    # TODO: Write a meaningful docstring about `summon`
    with _make_repo(repo, rev=rev) as _repo:

        def pull_dependencies(deps):
            if not deps:
                return

            outs = [_repo.find_out_by_relpath(dep) for dep in deps]

            with _repo.state:
                for out in outs:
                    _repo.cloud.pull(out.get_used_cache())
                    out.checkout()

        path = os.path.join(_repo.root_dir, fname)
        obj = _get_object_from_summoners_file(name, path)
        info = obj["summon"]

        pull_dependencies(info.get("deps"))

        _args = copy.deepcopy(info.get("args", {}))
        _args.update(args or {})

        return _invoke_method(info["call"], _args, path=_repo.root_dir)


def _get_object_from_summoners_file(name, path):
    """
    Given a summonable object's name, search for it on the given file
    and bring it to life.
    """
    schema = Schema(
        [
            {
                "name": str,
                "description": str,
                "paper": str,
                "metrics": dict,
                "summon": {
                    "type": "python",
                    "call": str,
                    "args": dict,  # XXX: Optional
                    "deps": [str],  # XXX: Optional
                },
            }
        ]
    )

    with open(path, "r") as fobj:
        try:
            objects = ruamel.yaml.safe_load(fobj.read())["objects"]
            objects = schema(objects)
            return next(x for x in objects if x["name"] == name)
        except FileNotFoundError:
            pass  # XXX: No such YAML file with path: '<path>'
        except ruamel.yaml.ScannerError:
            pass  # XXX: Failed to parse YAML correctly
        except KeyError:
            pass  # XXX: YAML file doesn't include the "objects" keyword
        except Invalid:
            pass  # XXX: YAML file dosen't match with the schema
        except StopIteration:
            pass  # XXX: No such object with name: '<name>'


def _invoke_method(call, args, path):
    # XXX: Some issues with this approach:
    #   * Not thread safe
    #   * Import will pollute sys.modules
    #   * Weird errors if there is a name clash within sys.modules
    cwd = os.path.abspath(os.curdir)

    try:
        os.chdir(path)
        sys.path.insert(0, path)
        method = _import_string(call)
        return method(**args)
    finally:
        os.chdir(cwd)
        sys.path.pop()


def _import_string(import_name, silent=False):
    """Imports an object based on a string.
    Useful to delay import to not load everything on startup.
    Use dotted notaion in `import_name`, e.g. 'dvc.remote.gs.RemoteGS'.
    If the `silent` is True the return value will be `None` if the import
    fails.

    :return: imported object
    """
    import_name = builtin_str(import_name)

    try:
        if "." in import_name:
            module, obj = import_name.rsplit(".", 1)
        else:
            return importlib.import_module(import_name)
        return getattr(importlib.import_module(module), obj)
    except (ImportError, AttributeError):
        if not silent:
            raise
