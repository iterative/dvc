"""Manages Git."""

import logging
import os
import shlex
from functools import partial

from funcy import cached_property
from pathspec.patterns import GitWildMatchPattern

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.progress import Tqdm
from dvc.scm.base import (
    Base,
    CloneError,
    FileNotInRepoError,
    RevError,
    SCMError,
)
from dvc.utils import fix_env, is_binary, relpath
from dvc.utils.fs import path_isin
from dvc.utils.serialize import dump_yaml, load_yaml

logger = logging.getLogger(__name__)


class TqdmGit(Tqdm):
    def update_git(self, op_code, cur_count, max_count=None, message=""):
        op_code = self.code2desc(op_code)
        if op_code:
            message = (op_code + " | " + message) if message else op_code
        if message:
            self.postfix["info"] = f" {message} | "
        self.update_to(cur_count, max_count)

    @staticmethod
    def code2desc(op_code):
        from git import RootUpdateProgress as OP

        ops = {
            OP.COUNTING: "Counting",
            OP.COMPRESSING: "Compressing",
            OP.WRITING: "Writing",
            OP.RECEIVING: "Receiving",
            OP.RESOLVING: "Resolving",
            OP.FINDING_SOURCES: "Finding sources",
            OP.CHECKING_OUT: "Checking out",
            OP.CLONE: "Cloning",
            OP.FETCH: "Fetching",
            OP.UPDWKTREE: "Updating working tree",
            OP.REMOVE: "Removing",
            OP.PATHCHANGE: "Changing path",
            OP.URLCHANGE: "Changing URL",
            OP.BRANCHCHANGE: "Changing branch",
        }
        return ops.get(op_code & OP.OP_MASK, "")


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"

    def __init__(self, root_dir=os.curdir, search_parent_directories=True):
        """Git class constructor.
        Requires `Repo` class from `git` module (from gitpython package).
        """
        super().__init__(root_dir)

        import git
        from git.exc import InvalidGitRepositoryError

        try:
            self.repo = git.Repo(
                root_dir, search_parent_directories=search_parent_directories
            )
        except InvalidGitRepositoryError:
            msg = "{} is not a git repository"
            raise SCMError(msg.format(root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        libpath = env.get("LD_LIBRARY_PATH", None)
        self.repo.git.update_environment(LD_LIBRARY_PATH=libpath)

        self.ignored_paths = []
        self.files_to_track = set()

    @property
    def root_dir(self):
        return self.repo.working_tree_dir

    @staticmethod
    def clone(url, to_path, rev=None, shallow_branch=None):
        import git

        ld_key = "LD_LIBRARY_PATH"

        env = fix_env(None)
        if is_binary() and ld_key not in env.keys():
            # In fix_env, we delete LD_LIBRARY_PATH key if it was empty before
            # PyInstaller modified it. GitPython, in git.Repo.clone_from, uses
            # env to update its own internal state. When there is no key in
            # env, this value is not updated and GitPython re-uses
            # LD_LIBRARY_PATH that has been set by PyInstaller.
            # See [1] for more info.
            # [1] https://github.com/gitpython-developers/GitPython/issues/924
            env[ld_key] = ""

        try:
            if shallow_branch is not None and os.path.exists(url):
                # git disables --depth for local clones unless file:// url
                # scheme is used
                url = f"file://{url}"
            with TqdmGit(desc="Cloning", unit="obj") as pbar:
                clone_from = partial(
                    git.Repo.clone_from,
                    url,
                    to_path,
                    env=env,  # needed before we can fix it in __init__
                    no_single_branch=True,
                    progress=pbar.update_git,
                )
                if shallow_branch is None:
                    tmp_repo = clone_from()
                else:
                    tmp_repo = clone_from(branch=shallow_branch, depth=1)
            tmp_repo.close()
        except git.exc.GitCommandError as exc:  # pylint: disable=no-member
            raise CloneError(url, to_path) from exc

        # NOTE: using our wrapper to make sure that env is fixed in __init__
        repo = Git(to_path)

        if rev:
            try:
                repo.checkout(rev)
            except git.exc.GitCommandError as exc:  # pylint: disable=no-member
                raise RevError(
                    "failed to access revision '{}' for repo '{}'".format(
                        rev, url
                    )
                ) from exc

        return repo

    @staticmethod
    def is_sha(rev):
        import git

        return rev and git.Repo.re_hexsha_shortened.search(rev)

    @staticmethod
    def _get_git_dir(root_dir):
        return os.path.join(root_dir, Git.GIT_DIR)

    @property
    def dir(self):
        return self.repo.git_dir

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

        if not path_isin(gitignore, os.path.realpath(self.root_dir)):
            raise FileNotInRepoError(path)

        return entry, gitignore

    def _ignored(self, path):
        from git.exc import GitCommandError

        try:
            self.repo.git.check_ignore(path)
            return True
        except GitCommandError:
            return False

    def ignore(self, path):
        entry, gitignore = self._get_gitignore(path)

        if self._ignored(path):
            return

        msg = "Adding '{}' to '{}'.".format(relpath(path), relpath(gitignore))
        logger.debug(msg)

        self._add_entry_to_gitignore(entry, gitignore)

        self.track_file(relpath(gitignore))

        self.ignored_paths.append(path)

    def _add_entry_to_gitignore(self, entry, gitignore):
        entry = GitWildMatchPattern.escape(entry)

        with open(gitignore, "a+", encoding="utf-8") as fobj:
            fobj.seek(0, os.SEEK_END)
            if fobj.tell() == 0:
                # Empty file
                prefix = ""
            else:
                fobj.seek(fobj.tell() - 1, os.SEEK_SET)
                last = fobj.read(1)
                prefix = "" if last == "\n" else "\n"
            fobj.write(f"{prefix}{entry}\n")

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

    def add(self, paths):
        # NOTE: GitPython is not currently able to handle index version >= 3.
        # See https://github.com/iterative/dvc/issues/610 for more details.
        try:
            self.repo.index.add(paths)
        except AssertionError:
            msg = (
                "failed to add '{}' to git. You can add those files "
                "manually using `git add`. See "
                "https://github.com/iterative/dvc/issues/610 for more "
                "details.".format(str(paths))
            )

            logger.exception(msg)

    def commit(self, msg):
        self.repo.index.commit(msg)

    def checkout(self, branch, create_new=False):
        if create_new:
            self.repo.git.checkout("HEAD", b=branch)
        else:
            self.repo.git.checkout(branch)

    def pull(self, **kwargs):
        infos = self.repo.remote().pull(**kwargs)
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError(f"pull failed: {info.note}")

    def push(self):
        infos = self.repo.remote().push()
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError(f"push failed: {info.summary}")

    def branch(self, branch):
        self.repo.git.branch(branch)

    def tag(self, tag):
        self.repo.git.tag(tag)

    def untracked_files(self):
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        return bool(self.repo.git.ls_files(path))

    def is_dirty(self):
        return self.repo.is_dirty()

    def active_branch(self):
        return self.repo.active_branch.name

    def list_branches(self):
        return [h.name for h in self.repo.heads]

    def list_tags(self):
        return [t.name for t in self.repo.tags]

    def list_all_commits(self):
        return [c.hexsha for c in self.repo.iter_commits("--all")]

    def _install_hook(self, name):
        hook = self._hook_path(name)
        with open(hook, "w+") as fobj:
            fobj.write(f"#!/bin/sh\nexec dvc git-hook {name} $@\n")

        os.chmod(hook, 0o777)

    def _install_merge_driver(self):
        self.repo.git.config("merge.dvc.name", "DVC merge driver")
        self.repo.git.config(
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
        config = load_yaml(config_path) if os.path.exists(config_path) else {}

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

        if entry in config["repos"]:
            return

        config["repos"].append(entry)
        dump_yaml(config_path, config)

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

    def track_file(self, path):
        self.files_to_track.add(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def get_tree(self, rev, **kwargs):
        from dvc.tree.git import GitTree

        return GitTree(self.repo, self.resolve_rev(rev), **kwargs)

    def get_rev(self):
        return self.repo.rev_parse("HEAD").hexsha

    def resolve_rev(self, rev):
        from contextlib import suppress

        from git.exc import BadName, GitCommandError

        def _resolve_rev(name):
            with suppress(BadName, GitCommandError):
                try:
                    # Try python implementation of rev-parse first, it's faster
                    return self.repo.rev_parse(name).hexsha
                except NotImplementedError:
                    # Fall back to `git rev-parse` for advanced features
                    return self.repo.git.rev_parse(name)
                except ValueError:
                    raise RevError(f"unknown Git revision '{name}'")

        # Resolve across local names
        sha = _resolve_rev(rev)
        if sha:
            return sha

        # Try all the remotes and if it resolves unambiguously then take it
        if not Git.is_sha(rev):
            shas = {
                _resolve_rev(f"{remote.name}/{rev}")
                for remote in self.repo.remotes
            } - {None}
            if len(shas) > 1:
                raise RevError(f"ambiguous Git revision '{rev}'")
            if len(shas) == 1:
                return shas.pop()

        raise RevError(f"unknown Git revision '{rev}'")

    def has_rev(self, rev):
        try:
            self.resolve_rev(rev)
            return True
        except RevError:
            return False

    def close(self):
        self.repo.close()

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
