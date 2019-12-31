import importlib
import os
import sys
import copy
from contextlib import contextmanager

try:
    from contextlib import _GeneratorContextManager as GCM
except ImportError:
    from contextlib import GeneratorContextManager as GCM

from dvc.utils.compat import urlparse

import ruamel.yaml
from voluptuous import Schema, Required, Invalid

from dvc.repo import Repo
from dvc.exceptions import DvcException, FileMissingError
from dvc.external_repo import external_repo


SUMMON_SCHEMA = Schema(
    {
        Required("objects"): [
            {
                Required("name"): str,
                "meta": dict,
                Required("summon"): {
                    Required("type"): "python",
                    Required("call"): str,
                    "args": dict,
                    "deps": [str],
                },
            }
        ]
    }
)


class SummonError(DvcException):
    pass


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


def summon(name, repo=None, rev=None, summon_file="dvcsummon.yaml", args=None):
    """Instantiate an object described in the summon file."""
    with _make_repo(repo, rev=rev) as _repo:
        try:
            path = os.path.join(_repo.root_dir, summon_file)
            obj = _get_object_from_summon_file(name, path)
            info = obj["summon"]
        except SummonError as exc:
            raise SummonError(
                str(exc) + " at '{}' in '{}'".format(summon_file, repo),
                cause=exc.cause,
            )

        _pull_dependencies(_repo, info.get("deps", []))

        _args = copy.deepcopy(info.get("args", {}))
        _args.update(args or {})

        return _invoke_method(info["call"], _args, path=_repo.root_dir)


def _get_object_from_summon_file(name, path):
    """
    Given a summonable object's name, search for it on the given file
    and return its description.
    """
    try:
        with open(path, "r") as fobj:
            content = SUMMON_SCHEMA(ruamel.yaml.safe_load(fobj.read()))
            objects = [x for x in content["objects"] if x["name"] == name]

        if not objects:
            raise SummonError("No object with name '{}'".format(name))
        elif len(objects) >= 2:
            raise SummonError(
                "More than one object with name '{}'".format(name)
            )

        return objects[0]

    except FileMissingError:
        raise SummonError("Summon file not found")
    except ruamel.yaml.YAMLError as exc:
        raise SummonError("Failed to parse summon file", exc)
    except Invalid as exc:
        raise SummonError(str(exc))


def _pull_dependencies(repo, deps):
    if not deps:
        return

    outs = [repo.find_out_by_relpath(dep) for dep in deps]

    with repo.state:
        for out in outs:
            repo.cloud.pull(out.get_used_cache())
            out.checkout()


def _invoke_method(call, args, path):
    # XXX: Some issues with this approach:
    #   * Not thread safe
    #   * Import will pollute sys.modules
    #   * Weird errors if there is a name clash within sys.modules

    # XXX: sys.path manipulation is "theoretically" not needed
    #      but tests are failing for an unknown reason.
    cwd = os.getcwd()

    try:
        os.chdir(path)
        sys.path.insert(0, path)
        method = _import_string(call)
        return method(**args)
    finally:
        os.chdir(cwd)
        sys.path.pop(0)


def _import_string(import_name):
    """Imports an object based on a string.
    Useful to delay import to not load everything on startup.
    Use dotted notaion in `import_name`, e.g. 'dvc.remote.gs.RemoteGS'.

    :return: imported object
    """
    if "." in import_name:
        module, obj = import_name.rsplit(".", 1)
    else:
        return importlib.import_module(import_name)
    return getattr(importlib.import_module(module), obj)
