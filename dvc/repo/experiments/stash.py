import re
from typing import Dict, Iterable, NamedTuple, Optional

from scmrepo.git import Stash


class ExpStashEntry(NamedTuple):
    """Experiment stash entry.

    stash_index: Stash index for this entry. Can be None if this commit
        is not pushed onto the stash ref.
    head_rev: HEAD Git commit to be checked out for this experiment.
    baseline_rev: Experiment baseline commit.
    branch: Optional exp (checkpoint) branch name for this experiment.
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
