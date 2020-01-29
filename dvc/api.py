from builtins import open as builtin_open
import os
from contextlib import contextmanager, _GeneratorContextManager as GCM

from funcy import cached_property, lmap
import ruamel.yaml
from voluptuous import Schema, Required, Invalid

from dvc.repo import Repo
from dvc.exceptions import DvcException, NotDvcRepoError
from dvc.external_repo import external_repo


class SummonError(DvcException):
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


class SummonFile(object):
    DEFAULT_FILENAME = "dvcsummon.yaml"
    SCHEMA = Schema(
        {
            Required("dvc-objects", default={}): {
                str: {
                    "meta": dict,
                    Required("summon"): {
                        Required("type"): str,
                        "deps": [str],
                        str: object,
                    },
                }
            }
        }
    )

    def __init__(self, repo_obj, summon_file):
        self.repo = repo_obj
        self.filename = summon_file
        self._path = os.path.join(self.repo.root_dir, summon_file)

    @staticmethod
    @contextmanager
    def prepare(repo=None, rev=None, summon_file=None):
        """Does a couple of things every summon needs as a prerequisite:
        clones the repo and parses the summon file.

        Calling code is expected to complete the summon logic following
        instructions stated in "summon" dict of the object spec.

        Returns a SummonFile instance, which contains references to a Repo
        object, named object specification and resolved paths to deps.
        """
        summon_file = summon_file or SummonFile.DEFAULT_FILENAME
        with _make_repo(repo, rev=rev) as _repo:
            _require_dvc(_repo)
            try:
                yield SummonFile(_repo, summon_file)
            except SummonError as exc:
                raise SummonError(
                    str(exc) + " at '{}' in '{}'".format(summon_file, _repo)
                ) from exc.__cause__

    @cached_property
    def objects(self):
        return self._read_yaml()["dvc-objects"]

    def _read_yaml(self):
        try:
            with builtin_open(self._path, mode="r") as fd:
                return self.SCHEMA(ruamel.yaml.safe_load(fd.read()))
        except FileNotFoundError as exc:
            raise SummonError("Summon file not found") from exc
        except ruamel.yaml.YAMLError as exc:
            raise SummonError("Failed to parse summon file") from exc
        except Invalid as exc:
            raise SummonError(str(exc)) from None

    def _write_yaml(self, objects):
        try:
            with builtin_open(self._path, "w") as fd:
                content = self.SCHEMA({"dvc-objects": objects})
                ruamel.yaml.safe_dump(content, fd)
        except Invalid as exc:
            raise SummonError(str(exc)) from None

    def abs(self, path):
        return os.path.join(self.repo.root_dir, path)

    def pull(self, targets):
        self.repo.pull([self.abs(target) for target in targets])

    def pull_deps(self, dobj):
        self.pull(dobj["summon"].get("deps", []))

    def get(self, name):
        """
        Given a summonable object's name, search for it this file
        and return its description.
        """
        if name not in self.objects:
            raise SummonError(
                "No object with name '{}' in '{}'".format(name, self.filename)
            )

        return self.objects[name]

    def set(self, name, dobj, overwrite=True):
        if not os.path.exists(self._path):
            self.objects = self.SCHEMA({})["dvc-objects"]

        if name in self.objects and not overwrite:
            raise SummonError(
                "There is an existing summonable object named '{}' in '{}:{}'."
                " Use SummonFile.set(..., overwrite=True) to"
                " overwrite it.".format(name, self.repo.url, self.filename)
            )

        self.objects[name] = dobj
        self._write_yaml(self.objects)

        # Add deps and push to remote
        deps = dobj["summon"].get("deps", [])
        stages = []
        if deps:
            stages = self.repo.add(
                lmap(self.abs, deps), fname=self.abs(name + ".dvc")
            )
            self.repo.push()

        # Create commit and push
        self.repo.scm.add([self._path] + [stage.path for stage in stages])
        self.repo.scm.commit("Add {} to {}".format(name, self.filename))
        self.repo.scm.push()


def _require_dvc(repo):
    if not isinstance(repo, Repo):
        raise UrlNotDvcRepoError(repo.url)
