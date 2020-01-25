from builtins import open as builtin_open
import importlib
import os
import sys
from contextlib import contextmanager, _GeneratorContextManager as GCM
import threading

from funcy import wrap_with
import ruamel.yaml
from voluptuous import Schema, Required, Invalid

from dvc.repo import Repo
from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.external_repo import external_repo

DEF_SUMMON = "Summon.yaml"

SUMMON_FILE_SCHEMA = Schema(
    {
        Required("objects"): [
            {
                Required("name"): str,
                "meta": dict,
                Required("summon"): {
                    Required("type"): str,
                    "deps": [str],
                    str: object,
                },
            }
        ]
    }
)
SUMMON_PYTHON_SCHEMA = Schema(
    {
        Required("type"): "python",
        Required("call"): str,
        "args": dict,
        "deps": [str],
    }
)


class SummonError(DvcException):
    pass


class SummonErrorNoObjectFound(SummonError):
    pass


class UrlNotDvcRepoError(DvcException):
    """Thrown if given url is not a DVC repository.

    Args:
        url (str): URL to the repository
    """

    def __init__(self, url):
        super().__init__("'{}' is not a DVC repository.".format(url))


def get_url(path, repo=None, rev=None, remote=None):
    """
    Returns the full URL to the data artifact specified by its `path` in a
    `repo`.
    NOTE: There is no guarantee that the file actually exists in that location.
    """
    with _make_repo(repo, rev=rev) as _repo:
        _require_dvc(_repo)
        out = _repo.find_out_by_relpath(path)
        remote_obj = _repo.cloud.get_remote(remote)
        return str(remote_obj.checksum_to_path_info(out.checksum))


def open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Context manager to open a file artifact as a file object."""
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
            "dvc.api.open() should be used in a with statement."
        )


def _open(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    with _make_repo(repo, rev=rev) as _repo:
        with _repo.open_by_relpath(
            path, remote=remote, mode=mode, encoding=encoding
        ) as fd:
            yield fd


def read(path, repo=None, rev=None, remote=None, mode="r", encoding=None):
    """Returns the contents of a file artifact."""
    with open(
        path, repo=repo, rev=rev, remote=remote, mode=mode, encoding=encoding
    ) as fd:
        return fd.read()


@contextmanager
def _make_repo(repo_url=None, rev=None):
    repo_url = repo_url or os.getcwd()
    if rev is None and os.path.exists(repo_url):
        try:
            yield Repo(repo_url)
            return
        except NotDvcRepoError:
            pass  # fallthrough to external_repo
    with external_repo(url=repo_url, rev=rev) as repo:
        yield repo


def summon(name, repo=None, rev=None, summon_file=DEF_SUMMON, args=None):
    """Instantiate an object described in the `summon_file`."""
    with SummonDesc.prepare_summon(repo, rev, summon_file) as desc:
        dobj = desc.get_dobject(name)
        try:
            summon_dict = SUMMON_PYTHON_SCHEMA(dobj["summon"])
        except Invalid as exc:
            raise SummonError(str(exc)) from exc

        desc.pull(dobj)
        _args = {**summon_dict.get("args", {}), **(args or {})}
        return _invoke_method(summon_dict["call"], _args, desc.repo.root_dir)


class SummonDesc(object):
    def __init__(self, repo_obj, summon_file=DEF_SUMMON):
        self.repo = repo_obj
        self.path = os.path.join(self.repo.root_dir, summon_file)
        self.summon_content = self._read_summon_content()

    def _read_summon_content(self):
        try:
            with builtin_open(self.path, "r") as fobj:
                return SUMMON_FILE_SCHEMA(ruamel.yaml.safe_load(fobj.read()))
        except FileNotFoundError as exc:
            raise SummonError("Summon file not found") from exc
        except ruamel.yaml.YAMLError as exc:
            raise SummonError("Failed to parse summon file") from exc
        except Invalid as exc:
            raise SummonError(str(exc)) from exc

    @staticmethod
    @contextmanager
    def prepare_summon(repo=None, rev=None, summon_file=DEF_SUMMON):
        """Does a couple of things every summon needs as a prerequisite:
        clones the repo and parses the summon file.

        Calling code is expected to complete the summon logic following
        instructions stated in "summon" dict of the object spec.

        Returns a SummonDesc instance, which contains references to a Repo
        object, named object specification and resolved paths to deps.
        """
        with _make_repo(repo, rev=rev) as _repo:
            _require_dvc(_repo)
            try:
                yield SummonDesc(_repo, summon_file)
            except SummonError as exc:
                raise SummonError(
                    str(exc) + " at '{}' in '{}'".format(summon_file, _repo)
                ) from exc.__cause__

    def deps_paths(self, dobj):
        return dobj["summon"].get("deps", [])

    def deps_abs_paths(self, dobj):
        return [
            os.path.join(self.repo.root_dir, p) for p in self.deps_paths(dobj)
        ]

    def outs(self, dobj):
        return [
            self.repo.find_out_by_relpath(d) for d in self.deps_paths(dobj)
        ]

    def pull(self, dobj):
        outs = self.outs(dobj)

        with self.repo.state:
            for out in outs:
                self.repo.cloud.pull(out.get_used_cache())
                out.checkout()

    # def to_abs_paths(self, paths):
    #     return [self.repo.find_out_by_relpath(d) for d in paths]

    def get_dobject(self, name):
        """
        Given a summonable object's name, search for it on the given content
        and return its description.
        """
        objects = [
            x for x in self.summon_content["objects"] if x["name"] == name
        ]

        if not objects:
            raise SummonErrorNoObjectFound(
                "No object with name '{}'".format(name)
            )
        elif len(objects) >= 2:
            raise SummonError(
                "More than one object with name '{}'".format(name)
            )

        return objects[0]

    def set_dobject(self, obj_new, overwrite=False):
        try:
            name = obj_new["name"]
            obj = self.get_dobject(name)

            if overwrite:
                idx = self.summon_content["objects"].index(obj)
                self.summon_content["objects"][idx] = obj_new
            else:
                raise SummonError("Object '{}' already exist".format(name))
        except SummonErrorNoObjectFound:
            self.summon_content["objects"].append(obj_new)


@wrap_with(threading.Lock())
def _invoke_method(call, args, path):
    # XXX: Some issues with this approach:
    #   * Import will pollute sys.modules
    #   * sys.path manipulation is "theoretically" not needed,
    #     but tests are failing for an unknown reason.
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


def _require_dvc(repo):
    if not isinstance(repo, Repo):
        raise UrlNotDvcRepoError(repo.url)
