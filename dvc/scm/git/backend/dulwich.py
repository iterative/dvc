import fnmatch
import logging
import os
from io import BytesIO
from typing import TYPE_CHECKING, Callable, Iterable, Optional, Tuple

from dvc.scm.base import SCMError
from dvc.utils import relpath

from .base import BaseGitBackend

if TYPE_CHECKING:
    from dvc.scm.git import Git

logger = logging.getLogger(__name__)


class DulwichBackend(BaseGitBackend):  # pylint:disable=abstract-method
    """Dulwich Git backend."""

    def __init__(self, scm: "Git", **kwargs):
        from dulwich.repo import Repo

        super().__init__(scm, **kwargs)
        self.repo = Repo(self.root_dir)
        self._stashes: dict = {}

    def close(self):
        self.repo.close()

    def _get_stash(self, ref: str):
        from dulwich.stash import Stash as DulwichStash

        if ref not in self._stashes:
            self._stashes[ref] = DulwichStash(self.repo, ref=os.fsencode(ref))
        return self._stashes[ref]

    def is_ignored(self, path):
        from dulwich import ignore

        manager = ignore.IgnoreFilterManager.from_repo(self.repo)
        return manager.is_ignored(relpath(path, self.root_dir))

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        name_b = os.fsencode(name)
        new_ref_b = os.fsencode(new_ref)
        old_ref_b = os.fsencode(old_ref) if old_ref else None
        message_b = message.encode("utf-8") if message else None
        if symbolic:
            return self.repo.refs.set_symbolic_ref(
                name_b, new_ref_b, message=message
            )
        if not self.repo.refs.set_if_equals(
            name_b, old_ref_b, new_ref_b, message=message_b
        ):
            raise SCMError(f"Failed to set '{name}'")

    def get_ref(self, name, follow: Optional[bool] = True) -> Optional[str]:
        from dulwich.refs import parse_symref_value

        name_b = os.fsencode(name)
        if follow:
            try:
                ref = self.repo.refs[name_b]
            except KeyError:
                ref = None
        else:
            ref = self.repo.refs.read_ref(name_b)
            try:
                if ref:
                    ref = parse_symref_value(ref)
            except ValueError:
                pass
        if ref:
            return os.fsdecode(ref)
        return None

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        name_b = name.encode("utf-8")
        old_ref_b = old_ref.encode("utf-8") if old_ref else None
        if not self.repo.refs.remove_if_equals(name_b, old_ref_b):
            raise SCMError(f"Failed to remove '{name}'")

    def iter_refs(self, base: Optional[str] = None):
        base_b = os.fsencode(base) if base else None
        for key in self.repo.refs.keys(base=base_b):
            if base:
                if base.endswith("/"):
                    base = base[:-1]
                yield "/".join([base, os.fsdecode(key)])
            else:
                yield os.fsdecode(key)

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        raise NotImplementedError

    def push_refspec(self, url: str, src: Optional[str], dest: str):
        from dulwich.client import get_transport_and_path
        from dulwich.objects import ZERO_SHA

        if src is not None and src.endswith("/"):
            src_b = os.fsencode(src)
            keys = self.repo.refs.subkeys(src_b)
            values = [self.repo.refs[b"".join([src_b, key])] for key in keys]
            dest_refs = [b"".join([os.fsencode(dest), key]) for key in keys]
        else:
            if src is None:
                values = [ZERO_SHA]
            else:
                values = [self.repo.refs[os.fsencode(src)]]
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
            self.repo.object_store.generate_pack_data,
            progress=progress,
        )

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Iterable[str],
        force: Optional[bool] = False,
        on_diverged: Optional[Callable[[bytes, bytes], bool]] = None,
    ):
        from dulwich.client import get_transport_and_path
        from dulwich.objectspec import parse_reftuples
        from dulwich.porcelain import DivergedBranches, check_diverged

        fetch_refs = []

        def determine_wants(remote_refs):
            fetch_refs.extend(
                parse_reftuples(
                    remote_refs,
                    self.repo.refs,
                    [os.fsencode(refspec) for refspec in refspecs],
                    force=force,
                )
            )
            return [
                remote_refs[lh]
                for (lh, _, _) in fetch_refs
                if remote_refs[lh] not in self.repo.object_store
            ]

        try:
            client, path = get_transport_and_path(url)
        except Exception as exc:

            raise SCMError("Could not get remote client") from exc

        def progress(msg):
            logger.trace("git fetch: %s", msg)

        fetch_result = client.fetch(
            path, self.repo, progress=progress, determine_wants=determine_wants
        )
        for (lh, rh, _) in fetch_refs:
            try:
                if rh in self.repo.refs:
                    check_diverged(
                        self.repo, self.repo.refs[rh], fetch_result.refs[lh]
                    )
            except DivergedBranches:
                if not force:
                    overwrite = False
                    if on_diverged:
                        overwrite = on_diverged(
                            os.fsdecode(rh), os.fsdecode(fetch_result.refs[lh])
                        )
                    if not overwrite:
                        continue
            self.repo.refs[rh] = fetch_result.refs[lh]

    def _stash_iter(self, ref: str):
        stash = self._get_stash(ref)
        yield from stash.stashes()

    def _stash_push(
        self,
        ref: str,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ) -> Tuple[Optional[str], bool]:
        from dvc.scm.git import Stash

        if include_untracked or ref == Stash.DEFAULT_STASH:
            # dulwich stash.push does not support include_untracked and does
            # not touch working tree
            raise NotImplementedError

        stash = self._get_stash(ref)
        message_b = message.encode("utf-8") if message else None
        stash.push(message=message_b)
        return os.fsdecode(stash[0].new_sha), True

    def _stash_apply(self, rev: str):
        raise NotImplementedError

    def reflog_delete(self, ref: str, updateref: bool = False):
        raise NotImplementedError

    def describe(
        self,
        rev: str,
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Optional[str]:
        if not base:
            base = "refs/tags"
        for ref in self.iter_refs(base=base):
            if (match and not fnmatch.fnmatch(ref, match)) or (
                exclude and fnmatch.fnmatch(ref, exclude)
            ):
                continue
            if self.scm.get_ref(ref, follow=False) == rev:
                return ref
        return None

    def diff(self, rev_a: str, rev_b: str, binary=False) -> str:
        from dulwich.patch import write_tree_diff

        commit_a = self.repo[os.fsencode(rev_a)]
        commit_b = self.repo[os.fsencode(rev_b)]

        buf = BytesIO()
        write_tree_diff(
            buf, self.repo.object_store, commit_a.tree, commit_b.tree
        )
        return buf.getvalue().decode("utf-8")
