"""Manages Git."""

import logging
import os
import shlex
from contextlib import contextmanager
from functools import partial
from typing import Iterable, Optional

from funcy import cached_property, first
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
from dvc.utils.fs import path_isin, remove
from dvc.utils.serialize import modify_yaml

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

        self._dulwich_repo = None

    @property
    def dulwich_repo(self):
        from dulwich.repo import Repo

        if self._dulwich_repo is None:
            self._dulwich_repo = Repo(self.root_dir)
        return self._dulwich_repo

    @property
    def root_dir(self) -> str:
        return self.repo.working_tree_dir

    @cached_property
    def stash(self):
        return Stash(self)

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

        if not path_isin(os.path.realpath(gitignore), self.root_dir):
            raise FileNotInRepoError(path)

        return entry, gitignore

    def is_ignored(self, path):
        from dulwich import ignore

        manager = ignore.IgnoreFilterManager.from_repo(self.dulwich_repo)
        return manager.is_ignored(relpath(path, self.root_dir))

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

    def checkout(self, branch, create_new=False, **kwargs):
        if create_new:
            self.repo.git.checkout("HEAD", b=branch, **kwargs)
        else:
            self.repo.git.checkout(branch, **kwargs)

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

    def is_dirty(self, **kwargs):
        return self.repo.is_dirty(**kwargs)

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
        if self._dulwich_repo is not None:
            self._dulwich_repo.close()
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

    def branch_revs(self, branch: str, end_rev: Optional[str] = None):
        """Iterate over revisions in a given branch (from newest to oldest).

        If end_rev is set, iterator will stop when the specified revision is
        reached.
        """
        commit = self.resolve_commit(branch)
        while commit is not None:
            yield commit.hexsha
            commit = first(commit.parents)
            if commit and commit.hexsha == end_rev:
                return

    def resolve_commit(self, rev):
        """Return Commit object for the specified revision."""
        from git.objects.tag import TagObject

        commit = self.repo.rev_parse(rev)
        if isinstance(commit, TagObject):
            commit = commit.object
        return commit

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        """Set the specified git ref.

        Optional kwargs:
            old_ref: If specified, ref will only be set if it currently equals
                old_ref. Has no effect is symbolic is True.
            message: Optional reflog message.
            symbolic: If True, ref will be set as a symbolic ref to new_ref
                rather than the dereferenced object.
        """
        name_b = os.fsencode(name)
        new_ref_b = os.fsencode(new_ref)
        old_ref_b = os.fsencode(old_ref) if old_ref else None
        message_b = message.encode("utf-8") if message else None
        if symbolic:
            return self.dulwich_repo.refs.set_symbolic_ref(
                name_b, new_ref_b, message=message
            )
        if not self.dulwich_repo.refs.set_if_equals(
            name_b, old_ref_b, new_ref_b, message=message_b
        ):
            raise SCMError(f"Failed to set '{name}'")

    def get_ref(self, name, follow: Optional[bool] = True):
        """Return the value of specified ref.

        If follow is false, symbolic refs will not be dereferenced.
        Returns None if the ref does not exist.
        """
        from dulwich.refs import parse_symref_value

        name = os.fsencode(name)
        if follow:
            try:
                ref = self.dulwich_repo.refs[name]
            except KeyError:
                ref = None
        else:
            ref = self.dulwich_repo.refs.read_ref(name)
            try:
                if ref:
                    ref = parse_symref_value(ref)
            except ValueError:
                pass
        if ref:
            ref = os.fsdecode(ref)
        return ref

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        """Remove the specified git ref.

        If old_ref is specified, ref will only be removed if it currently
        equals old_ref.
        """
        name_b = name.encode("utf-8")
        old_ref_b = old_ref.encode("utf-8") if old_ref else None
        if not self.dulwich_repo.refs.remove_if_equals(name_b, old_ref_b):
            raise SCMError(f"Failed to remove '{name}'")

    def get_refs_containing(self, rev: str, pattern: Optional[str]):
        """Iterate over all git refs containing the specfied revision."""
        from git.exc import GitCommandError

        try:
            if pattern:
                args = [pattern]
            else:
                args = []
            for line in self.repo.git.for_each_ref(
                *args, contains=rev, format=r"%(refname)"
            ).splitlines():
                line = line.strip()
                if line:
                    yield line
        except GitCommandError:
            pass

    def push_refspec(self, url: str, src: Optional[str], dest: str):
        """Push refspec to a remote Git repo.

        Args:
            url: Remote repo Git URL (Note this must be a Git URL and not
                a remote name).
            src: Local refspec. If src ends with "/" it will be treated as a
                prefix, and all refs inside src will be pushed using dest
                as destination refspec prefix. If src is None, dest will be
                deleted from the remote.
            dest: Remote refspec.
        """
        from dulwich.client import get_transport_and_path
        from dulwich.objects import ZERO_SHA

        if src is not None and src.endswith("/"):
            src_b = os.fsencode(src)
            keys = self.dulwich_repo.refs.subkeys(src_b)
            values = [
                self.dulwich_repo.refs[b"".join([src_b, key])] for key in keys
            ]
            dest_refs = [b"".join([os.fsencode(dest), key]) for key in keys]
        else:
            if src is None:
                values = [ZERO_SHA]
            else:
                values = [self.dulwich_repo.refs[os.fsencode(src)]]
            dest_refs = [os.fsencode(dest)]

        def update_refs(refs):
            for ref, value in zip(dest_refs, values):
                refs[ref] = value
            return refs

        try:
            client, path = get_transport_and_path(url)
        except Exception as exc:
            raise SCMError("Could not get remote client") from exc

        def progress(msg):
            logger.trace("git send_pack: %s", msg)

        client.send_pack(
            path,
            update_refs,
            self.dulwich_repo.object_store.generate_pack_data,
            progress=progress,
        )

    def fetch_refspecs(
        self, url: str, refspecs: Iterable[str], force: Optional[bool] = False
    ):
        """Fetch refspecs from a remote Git repo.

        Args:
            url: Remote repo Git URL (Note this must be a Git URL and not
                a remote name).
            refspecs: Iterable containing refspecs to fetch.
                Note that this will not match subkeys.
            force: If True, local refs will be overwritten.
        """
        from dulwich.client import get_transport_and_path
        from dulwich.objectspec import parse_reftuples
        from dulwich.porcelain import DivergedBranches, check_diverged

        fetch_refs = []
        repo = self.dulwich_repo

        def determine_wants(remote_refs):
            fetch_refs.extend(
                parse_reftuples(
                    remote_refs,
                    repo.refs,
                    [os.fsencode(refspec) for refspec in refspecs],
                    force=force,
                )
            )
            return [
                remote_refs[lh]
                for (lh, _, _) in fetch_refs
                if remote_refs[lh] not in repo.object_store
            ]

        try:
            client, path = get_transport_and_path(url)
        except Exception as exc:
            raise SCMError("Could not get remote client") from exc

        def progress(msg):
            logger.trace("git fetch: %s", msg)

        fetch_result = client.fetch(
            path, repo, progress=progress, determine_wants=determine_wants
        )
        for (lh, rh, _) in fetch_refs:
            try:
                if rh in repo.refs:
                    check_diverged(repo, repo.refs[rh], fetch_result.refs[lh])
            except DivergedBranches as exc:
                raise SCMError("Experiment branch has diverged") from exc
            repo.refs[rh] = fetch_result.refs[lh]

    @contextmanager
    def detach_head(self, rev: Optional[str] = None):
        """Context manager for performing detached HEAD SCM operations.

        Detaches and restores HEAD similar to interactive git rebase.
        Restore is equivalent to 'reset --soft', meaning the caller is
        is responsible for preserving & restoring working tree state
        (i.e. via stash) when applicable.

        Yields revision of detached head.
        """
        from dulwich.refs import LOCAL_BRANCH_PREFIX

        if not rev:
            rev = "HEAD"
        orig_head = self.get_ref("HEAD", follow=False)
        logger.debug("Detaching HEAD at '%s'", rev)
        self.checkout(rev, detach=True)
        try:
            yield self.get_ref("HEAD")
        finally:
            prefix = os.fsdecode(LOCAL_BRANCH_PREFIX)
            if orig_head.startswith(prefix):
                orig_head = orig_head[len(prefix) :]
            logger.debug("Restore HEAD to '%s'", orig_head)
            self.checkout(orig_head)

    @contextmanager
    def stash_workspace(self, **kwargs):
        """Stash restore any workspace changes.

        Yields revision of the stash commit.
        """
        logger.debug("Stashing workspace")
        rev = self.stash.push(**kwargs)
        try:
            yield rev
        finally:
            logger.debug("Restoring stashed workspace")
            self.stash.pop()


class Stash:
    """Wrapper for representing Git stash.

    Uses dulwich.stash when possible, `git stash` will be used directly for
    operations which are not implemented in dulwich.
    """

    DEFAULT_STASH = "refs/stash"

    def __init__(self, scm, ref: Optional[str] = None):
        from dulwich.stash import Stash as DulwichStash

        self.ref = ref if ref else self.DEFAULT_STASH
        self.scm = scm
        self._stash = DulwichStash(
            self.scm.dulwich_repo, ref=os.fsencode(self.ref)
        )

    @property
    def git(self):
        return self.scm.repo.git

    def __iter__(self):
        yield from self._stash.stashes()

    def __len__(self):
        return len(self._stash)

    def __getitem__(self, index):
        return self._stash.__getitem__(index)

    def list(self):
        return self._stash.stashes()

    def push(
        self,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ):
        # dulwich stash.push does not support include_untracked and does not
        # touch working tree
        logger.debug("Stashing changes in '%s'", self.ref)
        if include_untracked:
            self._git_push(
                message=message, include_untracked=include_untracked
            )
        else:
            message_b = message.encode("utf-8") if message else None
            self._stash.push(message=message_b)
            self.git.reset(hard=True)
        return os.fsdecode(self[0].new_sha)

    def _git_push(
        self,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ):
        args = ["push"]
        if message:
            args.extend(["-m", message])
        if include_untracked:
            args.append("--include-untracked")
        self.git.stash(*args)
        if self.ref != self.DEFAULT_STASH:
            # `git stash` CLI doesn't support using custom refspecs,
            # so we push a commit onto refs/stash, make our refspec
            # point to the new commit, then pop it from refs/stash
            # `git stash create` is intended to be used for this kind of
            # behavior but it doesn't support --include-untracked so we need to
            # use push
            commit = self.scm.resolve_commit("stash@{0}")
            self.scm.set_ref(self.ref, commit.hexsha, message=commit.message)
            self.git.stash("drop")

    def pop(self):
        logger.debug("Popping from stash '%s'", self.ref)
        rev = os.fsdecode(self[0].new_sha)
        if self.ref == self.DEFAULT_STASH:
            self.git.stash("pop")
        else:
            self.apply(rev)
            self.drop()
        return rev

    def apply(self, rev):
        logger.debug("Applying stash commit '%s'", rev)
        self.git.stash("apply", rev)

    def drop(self, index: int = 0):
        ref = "{0}@{{{1}}}".format(self.ref, index)
        if index < 0 or index >= len(self):
            raise SCMError(f"Invalid stash ref '{ref}'")
        logger.debug("Dropping '%s'", ref)
        self.git.reflog("delete", "--updateref", ref)

        # if we removed the last reflog entry, delete the ref and reflog
        if len(self) == 0:
            self.scm.remove_ref(self.ref)
            parts = self.ref.split("/")
            reflog = os.path.join(self.scm.root_dir, ".git", "logs", *parts)
            remove(reflog)

    def clear(self):
        logger.debug("Clear stash '%s'", self.ref)
        for _ in range(len(self)):
            self.drop()
