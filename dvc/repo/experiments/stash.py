import re
from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, NamedTuple, Optional

from scmrepo.git import Stash

from dvc.exceptions import DvcException
from dvc.log import logger
from dvc_objects.fs.local import localfs
from dvc_objects.fs.utils import as_atomic

from .refs import APPLY_HEAD, APPLY_STASH

logger = logger.getChild(__name__)


class ExpStashEntry(NamedTuple):
    """Experiment stash entry.

    stash_index: Stash index for this entry. Can be None if this commit
        is not pushed onto the stash ref.
    head_rev: HEAD Git commit to be checked out for this experiment.
    baseline_rev: Experiment baseline commit.
    branch: Optional branch name for this experiment.
    name: Optional exp name.
    """

    stash_index: Optional[int]
    head_rev: str
    baseline_rev: str
    branch: Optional[str]
    name: Optional[str]


class ExpStash(Stash):
    MESSAGE_FORMAT = "dvc-exp:{rev}:{baseline_rev}:{name}"
    MESSAGE_RE = re.compile(
        r"(?:commit: )"
        r"dvc-exp:(?P<rev>[0-9a-f]+):(?P<baseline_rev>[0-9a-f]+)"
        r":(?P<name>[^~^:\\?\[\]*]*)"
        r"(:(?P<branch>.+))?$"
    )

    @property
    def stash_revs(self) -> Dict[str, ExpStashEntry]:
        revs = {}
        for i, entry in enumerate(self):
            msg = entry.message.decode("utf-8").strip()
            m = self.MESSAGE_RE.match(msg)
            if m:
                revs[entry.new_sha.decode("utf-8")] = ExpStashEntry(
                    i,
                    m.group("rev"),
                    m.group("baseline_rev"),
                    m.group("branch"),
                    m.group("name"),
                )
        return revs

    @classmethod
    def format_message(
        cls,
        rev: str,
        baseline_rev: str,
        name: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> str:
        msg = cls.MESSAGE_FORMAT.format(
            rev=rev, baseline_rev=baseline_rev, name=name if name else ""
        )
        branch_msg = f":{branch}" if branch else ""
        return f"{msg}{branch_msg}"

    def remove_revs(self, stash_revs: Iterable[ExpStashEntry]):
        """Remove the specified entries from the queue by stash revision."""
        for index in sorted(
            (
                entry.stash_index
                for entry in stash_revs
                if entry.stash_index is not None
            ),
            reverse=True,
        ):
            self.drop(index)


class ApplyStashEntry(NamedTuple):
    """Apply stash entry.

    stash_index: Stash index for this entry. Can be None if this commit
        is not pushed onto the stash ref.
    head_rev: HEAD Git commit prior to exp apply.
    rev: Applied experiment commit.
    name: Optional applied exp name.
    """

    stash_index: Optional[int]
    head_rev: str
    rev: str
    name: Optional[str]


class ApplyStash(Stash):
    DEFAULT_STASH = APPLY_STASH
    MESSAGE_FORMAT = "dvc-exp-apply:{head_rev}:{rev}:{name}"
    MESSAGE_RE = re.compile(
        r"(?:commit: )"
        r"dvc-exp-apply:(?P<head_rev>[0-9a-f]+):(?P<rev>[0-9a-f]+)"
        r":(?P<name>[^~^:\\?\[\]*]*)"
    )

    @property
    def stash_revs(self) -> Dict[str, ApplyStashEntry]:
        revs = {}
        for i, entry in enumerate(self):
            msg = entry.message.decode("utf-8").strip()
            m = self.MESSAGE_RE.match(msg)
            if m:
                revs[entry.new_sha.decode("utf-8")] = ApplyStashEntry(
                    i,
                    m.group("head_rev"),
                    m.group("rev"),
                    m.group("name"),
                )
        return revs

    @classmethod
    def format_message(
        cls,
        head_rev: str,
        rev: str,
        name: Optional[str] = None,
    ) -> str:
        return cls.MESSAGE_FORMAT.format(
            head_rev=head_rev, rev=rev, name=name if name else ""
        )

    @contextmanager
    def preserve_workspace(
        self, rev: str, name: Optional[str] = None
    ) -> Iterator[Optional[str]]:
        if len(self):
            logger.debug("Clearing existing exp-apply stash")
            self.clear()
        head = self.scm.get_rev()
        self.scm.set_ref(APPLY_HEAD, head)
        message = self.format_message(head, rev, name=name)
        stash_rev = self.push(message=message, include_untracked=True)
        try:
            yield stash_rev
            if stash_rev:
                self._apply_difference(stash_rev, rev)
        except Exception:
            self.revert_workspace()
            raise

    def _apply_difference(self, stash_rev: str, rev: str):
        """Selectively apply changes from stash_rev.

        Only changes to files from left which do not exist in right will be applied.
        """
        self._copy_difference(stash_rev, rev)
        commit = self.scm.resolve_commit(stash_rev)
        for parent_rev in commit.parents:
            parent_commit = self.scm.resolve_commit(parent_rev)
            if parent_commit.message.startswith("untracked files on "):
                self._copy_difference(parent_rev, rev)

    def _copy_difference(self, left_rev: str, right_rev: str):
        left_fs = self.scm.get_fs(left_rev)
        right_fs = self.scm.get_fs(right_rev)
        paths = [path for path in left_fs.find("/") if not right_fs.exists(path)]
        dest_paths = [
            localfs.join(self.scm.root_dir, left_fs.relpath(path, "/"))
            for path in paths
        ]
        for src, dest in zip(paths, dest_paths):
            with as_atomic(localfs, dest, create_parents=True) as tmp_file:
                left_fs.get_file(src, tmp_file)

    def revert_workspace(self):
        apply_head = self.scm.get_ref(self.ref)
        head = self.scm.get_rev()
        if apply_head != head:
            raise DvcException(
                f"Cannot revert workspace, current HEAD '{head[:7]}' does not match the"
                f" pre-apply HEAD '{apply_head[:7]}'"
            )
        self.scm.reset(hard=True)
        if len(self):
            # In the event that the apply-stash and current workspace contain
            # conflicting untracked files, we do:
            #   1. stash the current untracked files
            #   2. restore/pop the apply-stash (with untracked files)
            #   3. restore/pop the untracked files from (1) and ignore any conflicts
            #      (forcefully reverting to the apply-stash version)
            workspace_rev = self.scm.stash.push(include_untracked=True)
            try:
                self.pop()
            finally:
                if workspace_rev:
                    self.scm.stash.pop(skip_conflicts=True)
        self.scm.remove_ref(self.ref)
