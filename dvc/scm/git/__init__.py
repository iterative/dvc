"""Manages Git."""

import logging
import os

from funcy import cached_property
from pathspec.patterns import GitWildMatchPattern

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.scm.base import Base
from dvc.scm.base import CloneError, FileNotInRepoError, RevError, SCMError
from dvc.scm.git.tree import GitTree
from dvc.utils import fix_env, is_binary, relpath
from dvc.utils.fs import path_isin

logger = logging.getLogger(__name__)


class Git(Base):
    """Class for managing Git."""

    GITIGNORE = ".gitignore"
    GIT_DIR = ".git"

    def __init__(self, root_dir=os.curdir):
        """Git class constructor.
        Requires `Repo` class from `git` module (from gitpython package).
        """
        super().__init__(root_dir)

        import git
        from git.exc import InvalidGitRepositoryError

        try:
            self.repo = git.Repo(self.root_dir)
        except InvalidGitRepositoryError:
            msg = "{} is not a git repository"
            raise SCMError(msg.format(self.root_dir))

        # NOTE: fixing LD_LIBRARY_PATH for binary built by PyInstaller.
        # http://pyinstaller.readthedocs.io/en/stable/runtime-information.html
        env = fix_env(None)
        libpath = env.get("LD_LIBRARY_PATH", None)
        self.repo.git.update_environment(LD_LIBRARY_PATH=libpath)

        self.ignored_paths = []
        self.files_to_track = set()

    @staticmethod
    def clone(url, to_path, rev=None):
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
            tmp_repo = git.Repo.clone_from(
                url,
                to_path,
                env=env,  # needed before we can fix it in __init__
                no_single_branch=True,
            )
            tmp_repo.close()
        except git.exc.GitCommandError as exc:
            raise CloneError(url, to_path) from exc

        # NOTE: using our wrapper to make sure that env is fixed in __init__
        repo = Git(to_path)

        if rev:
            try:
                repo.checkout(rev)
            except git.exc.GitCommandError as exc:
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
    def is_repo(root_dir):
        return os.path.isdir(Git._get_git_dir(root_dir))

    @staticmethod
    def is_submodule(root_dir):
        return os.path.isfile(Git._get_git_dir(root_dir))

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
            fobj.write("{}{}\n".format(prefix, entry))

    def ignore_remove(self, path):
        entry, gitignore = self._get_gitignore(path)

        if not os.path.exists(gitignore):
            return

        with open(gitignore, "r") as fobj:
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

    def pull(self):
        infos = self.repo.remote().pull()
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError("pull failed: {}".format(info.note))

    def push(self):
        infos = self.repo.remote().push()
        for info in infos:
            if info.flags & info.ERROR:
                raise SCMError("push failed: {}".format(info.summary))

    def branch(self, branch):
        self.repo.git.branch(branch)

    def tag(self, tag):
        self.repo.git.tag(tag)

    def untracked_files(self):
        files = self.repo.untracked_files
        return [os.path.join(self.repo.working_dir, fname) for fname in files]

    def is_tracked(self, path):
        # it is equivalent to `bool(self.repo.git.ls_files(path))` by
        # functionality, but ls_files fails on unicode filenames
        path = relpath(path, self.root_dir)
        # There are 4 stages, see BaseIndexEntry.stage
        return any((path, i) in self.repo.index.entries for i in (0, 1, 2, 3))

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

    def _install_hook(self, name, preconditions, cmd):
        # only run in dvc repo
        in_dvc_repo = '[ -n "$(git ls-files --full-name .dvc)" ]'

        command = "if {}; then exec dvc {}; fi".format(
            " && ".join([in_dvc_repo] + preconditions), cmd
        )

        hook = self._hook_path(name)

        if os.path.isfile(hook):
            with open(hook, "r+") as fobj:
                if command not in fobj.read():
                    fobj.write("{command}\n".format(command=command))
        else:
            with open(hook, "w+") as fobj:
                fobj.write("#!/bin/sh\n" "{command}\n".format(command=command))

        os.chmod(hook, 0o777)

    def install(self):
        self._verify_dvc_hooks()

        self._install_hook(
            "post-checkout",
            [
                # checking out some reference and not specific file.
                '[ "$3" = "1" ]',
                # make sure we are not in the middle of a rebase/merge, so we
                # don't accidentally break it with an unsuccessful checkout.
                # Note that git hooks are always running in repo root.
                "[ ! -d .git/rebase-merge ]",
            ],
            "checkout",
        )
        self._install_hook("pre-commit", [], "status")
        self._install_hook("pre-push", [], "push")

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

        logger.info(
            "\n"
            "To track the changes with git, run:\n"
            "\n"
            "\tgit add {files}".format(files=" ".join(self.files_to_track))
        )

    def track_file(self, path):
        self.files_to_track.add(path)

    def belongs_to_scm(self, path):
        basename = os.path.basename(path)
        path_parts = os.path.normpath(path).split(os.path.sep)
        return basename == self.ignore_file or Git.GIT_DIR in path_parts

    def get_tree(self, rev):
        return GitTree(self.repo, self.resolve_rev(rev))

    def get_rev(self):
        return self.repo.rev_parse("HEAD").hexsha

    def resolve_rev(self, rev):
        from git.exc import BadName, GitCommandError
        from contextlib import suppress

        def _resolve_rev(name):
            with suppress(BadName, GitCommandError):
                try:
                    # Try python implementation of rev-parse first, it's faster
                    return self.repo.rev_parse(name).hexsha
                except NotImplementedError:
                    # Fall back to `git rev-parse` for advanced features
                    return self.repo.git.rev_parse(name)

        # Resolve across local names
        sha = _resolve_rev(rev)
        if sha:
            return sha

        # Try all the remotes and if it resolves unambiguously then take it
        if not Git.is_sha(rev):
            shas = {
                _resolve_rev("{}/{}".format(remote.name, rev))
                for remote in self.repo.remotes
            } - {None}
            if len(shas) > 1:
                raise RevError("ambiguous Git revision '{}'".format(rev))
            if len(shas) == 1:
                return shas.pop()

        raise RevError("unknown Git revision '{}'".format(rev))

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
