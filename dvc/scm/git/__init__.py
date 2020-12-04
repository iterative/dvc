"""Manages Git."""

import logging
import os
import shlex
from collections import OrderedDict
from contextlib import contextmanager
from functools import partialmethod
from typing import Optional

from funcy import cached_property
from pathspec.patterns import GitWildMatchPattern

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.scm.base import Base, FileNotInRepoError, RevError
from dvc.utils import relpath
from dvc.utils.fs import path_isin
from dvc.utils.serialize import modify_yaml

from .backend.base import NoGitBackendError
from .backend.dulwich import DulwichBackend
from .backend.gitpython import GitPythonBackend
from .stash import Stash

logger = logging.getLogger(__name__)


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"
    DEFAULT_BACKENDS = OrderedDict(
        [("dulwich", DulwichBackend), ("gitpython", GitPythonBackend)]
    )
    LOCAL_BRANCH_PREFIX = "refs/heads/"

    def __init__(self, *args, **kwargs):
        self.ignored_paths = []
        self.files_to_track = set()

        self.backends = OrderedDict(
            [
                (name, cls(self, *args, **kwargs))
                for name, cls in self.DEFAULT_BACKENDS.items()
            ]
        )

        super().__init__(self.gitpython.root_dir)

    @property
    def dir(self):
        return self.gitpython.repo.git_dir

    @property
    def gitpython(self):
        return self.backends["gitpython"]

    @property
    def dulwich(self):
        return self.backends["dulwich"]

    @cached_property
    def stash(self):
        return Stash(self)

    @classmethod
    def clone(cls, url, to_path, **kwargs):
        for _, backend in cls.DEFAULT_BACKENDS.items():
            try:
                backend.clone(url, to_path, **kwargs)
                return Git(to_path)
            except NotImplementedError:
                pass
        raise NoGitBackendError("clone")

    @classmethod
    def is_sha(cls, rev):
        for _, backend in cls.DEFAULT_BACKENDS.items():
            try:
                return backend.is_sha(rev)
            except NotImplementedError:
                pass
        raise NoGitBackendError("is_sha")

    @staticmethod
    def _get_git_dir(root_dir):
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def ignore_file(self):
        return self.GITIGNORE

    def _get_gitignore(self, path):
        ignore_file_dir = os.path.dirname(path)

        assert os.path.isabs(path)
        assert os.path.isabs(ignore_file_dir)

        entry = relpath(path, ignore_file_dir).replace(os.sep, "/")
        # NOTE: using '/' prefix to make path unambiguous
        if len(entry) > 0 and entry[0] != "/":
            entry = "/" + entry

        gitignore = os.path.join(ignore_file_dir, self.GITIGNORE)

        if not path_isin(os.path.realpath(gitignore), self.root_dir):
            raise FileNotInRepoError(path)

        return entry, gitignore

    def ignore(self, path):
        entry, gitignore = self._get_gitignore(path)

        if self.is_ignored(path):
            return

        msg = "Adding '{}' to '{}'.".format(relpath(path), relpath(gitignore))
        logger.debug(msg)

        self._add_entry_to_gitignore(entry, gitignore)

        self.track_file(relpath(gitignore))

        self.ignored_paths.append(path)

    def _add_entry_to_gitignore(self, entry, gitignore):
        entry = GitWildMatchPattern.escape(entry)

        with open(gitignore, "a+", encoding="utf-8") as fobj:
            unique_lines = set(fobj.readlines())
            fobj.seek(0, os.SEEK_END)
            if fobj.tell() == 0:
                # Empty file
                prefix = ""
            else:
                fobj.seek(fobj.tell() - 1, os.SEEK_SET)
                last = fobj.read(1)
                prefix = "" if last == "\n" else "\n"
            new_entry = f"{prefix}{entry}\n"
            if new_entry not in unique_lines:
                fobj.write(new_entry)

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore) as fobj:
            lines = fobj.readlines()

        filtered = list(filter(lambda x: x.strip() != entry.strip(), lines))

        with open(gitignore, "w") as fobj:
            fobj.writelines(filtered)

        self.track_file(relpath(gitignore))

    def _install_hook(self, name):
        hook = self._hook_path(name)
        with open(hook, "w+") as fobj:
            fobj.write(f"#!/bin/sh\nexec dvc git-hook {name} $@\n")

        os.chmod(hook, 0o777)

    def _install_merge_driver(self):
        self.gitpython.repo.git.config("merge.dvc.name", "DVC merge driver")
        self.gitpython.repo.git.config(
            "merge.dvc.driver",
            (
                "dvc git-hook merge-driver "
                "--ancestor %O "
                "--our %A "
                "--their %B "
            ),
        )

    def install(self, use_pre_commit_tool=False):
        self._install_merge_driver()

        if not use_pre_commit_tool:
            self._verify_dvc_hooks()
            self._install_hook("post-checkout")
            self._install_hook("pre-commit")
            self._install_hook("pre-push")
            return

        config_path = os.path.join(self.root_dir, ".pre-commit-config.yaml")
        with modify_yaml(config_path) as config:
            entry = {
                "repo": "https://github.com/iterative/dvc",
                "rev": "master",
                "hooks": [
                    {
                        "id": "dvc-pre-commit",
                        "language_version": "python3",
                        "stages": ["commit"],
                    },
                    {
                        "id": "dvc-pre-push",
                        "language_version": "python3",
                        "stages": ["push"],
                    },
                    {
                        "id": "dvc-post-checkout",
                        "language_version": "python3",
                        "stages": ["post-checkout"],
                        "always_run": True,
                    },
                ],
            }

            if entry not in config["repos"]:
                config["repos"].append(entry)

    def cleanup_ignores(self):
        for path in self.ignored_paths:
            self.ignore_remove(path)
        self.reset_ignores()

    def reset_ignores(self):
        self.ignored_paths = []

    def reset_tracked_files(self):
        self.files_to_track = set()

    def remind_to_track(self):
        if not self.files_to_track:
            return

        files = " ".join(shlex.quote(path) for path in self.files_to_track)

        logger.info(
            "\n"
            "To track the changes with git, run:\n"
            "\n"
            "\tgit add {files}".format(files=files)
        )

    def track_changed_files(self):
        if not self.files_to_track:
            return

        self.add(self.files_to_track)

    def track_file(self, path):
        self.files_to_track.add(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def has_rev(self, rev):
        try:
            self.resolve_rev(rev)
            return True
        except RevError:
            return False

    def close(self):
        for backend in self.backends.values():
            backend.close()

    @cached_property
    def _hooks_home(self):
        return os.path.join(self.root_dir, self.GIT_DIR, "hooks")

    def _hook_path(self, name):
        return os.path.join(self._hooks_home, name)

    def _verify_hook(self, name):
        if os.path.exists(self._hook_path(name)):
            raise GitHookAlreadyExistsError(name)

    def _verify_dvc_hooks(self):
        self._verify_hook("post-checkout")
        self._verify_hook("pre-commit")
        self._verify_hook("pre-push")

    @property
    def no_commits(self):
        return not self.list_all_commits()

    def _backend_func(self, name, *args, **kwargs):
        for backend in self.backends.values():
            try:
                func = getattr(backend, name)
                return func(*args, **kwargs)
            except NotImplementedError:
                pass
        raise NoGitBackendError(name)

    is_ignored = partialmethod(_backend_func, "is_ignored")
    add = partialmethod(_backend_func, "add")
    commit = partialmethod(_backend_func, "commit")
    checkout = partialmethod(_backend_func, "checkout")
    pull = partialmethod(_backend_func, "pull")
    push = partialmethod(_backend_func, "push")
    branch = partialmethod(_backend_func, "branch")
    tag = partialmethod(_backend_func, "tag")
    untracked_files = partialmethod(_backend_func, "untracked_files")
    is_tracked = partialmethod(_backend_func, "is_tracked")
    is_dirty = partialmethod(_backend_func, "is_dirty")
    active_branch = partialmethod(_backend_func, "active_branch")
    list_branches = partialmethod(_backend_func, "list_branches")
    list_tags = partialmethod(_backend_func, "list_tags")
    list_all_commits = partialmethod(_backend_func, "list_all_commits")
    get_tree = partialmethod(_backend_func, "get_tree")
    get_rev = partialmethod(_backend_func, "get_rev")
    _resolve_rev = partialmethod(_backend_func, "resolve_rev")
    resolve_commit = partialmethod(_backend_func, "resolve_commit")
    branch_revs = partialmethod(_backend_func, "branch_revs")

    set_ref = partialmethod(_backend_func, "set_ref")
    get_ref = partialmethod(_backend_func, "get_ref")
    remove_ref = partialmethod(_backend_func, "remove_ref")
    iter_refs = partialmethod(_backend_func, "iter_refs")
    get_refs_containing = partialmethod(_backend_func, "get_refs_containing")
    push_refspec = partialmethod(_backend_func, "push_refspec")
    fetch_refspecs = partialmethod(_backend_func, "fetch_refspecs")
    _stash_iter = partialmethod(_backend_func, "_stash_iter")
    _stash_push = partialmethod(_backend_func, "_stash_push")
    _stash_apply = partialmethod(_backend_func, "_stash_apply")
    reflog_delete = partialmethod(_backend_func, "reflog_delete")
    describe = partialmethod(_backend_func, "describe")
    diff = partialmethod(_backend_func, "diff")

    def resolve_rev(self, rev: str) -> str:
        from dvc.repo.experiments.utils import exp_refs_by_name

        try:
            return self._resolve_rev(rev)
        except RevError:
            # backends will only resolve git branch and tag names,
            # if rev is not a sha it may be an abbreviated experiment name
            if not self.is_sha(rev) and not rev.startswith("refs/"):
                ref_infos = list(exp_refs_by_name(self, rev))
                if len(ref_infos) == 1:
                    return self.get_ref(str(ref_infos[0]))
                if len(ref_infos) > 1:
                    raise RevError(f"ambiguous Git revision '{rev}'")
            raise

    @contextmanager
    def detach_head(self, rev: Optional[str] = None):
        """Context manager for performing detached HEAD SCM operations.

        Detaches and restores HEAD similar to interactive git rebase.
        Restore is equivalent to 'reset --soft', meaning the caller is
        is responsible for preserving & restoring working tree state
        (i.e. via stash) when applicable.

        Yields revision of detached head.
        """
        if not rev:
            rev = "HEAD"
        orig_head = self.get_ref("HEAD", follow=False)
        logger.debug("Detaching HEAD at '%s'", rev)
        self.checkout(rev, detach=True)
        try:
            yield self.get_ref("HEAD")
        finally:
            prefix = self.LOCAL_BRANCH_PREFIX
            if orig_head.startswith(prefix):
                orig_head = orig_head[len(prefix) :]
            logger.debug("Restore HEAD to '%s'", orig_head)
            self.checkout(orig_head)

    @contextmanager
    def stash_workspace(self, **kwargs):
        """Stash and restore any workspace changes.

        Yields revision of the stash commit. Yields None if there were no
        changes to stash.
        """
        logger.debug("Stashing workspace")
        rev = self.stash.push(**kwargs)
        try:
            yield rev
        finally:
            if rev:
                logger.debug("Restoring stashed workspace")
                self.stash.pop()
