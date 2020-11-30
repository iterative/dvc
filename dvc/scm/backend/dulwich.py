import logging
import os
from typing import TYPE_CHECKING, Iterable, Optional, Union

from dvc.scm.backend.base import GitBackend
from dvc.scm.base import SCMError
from dvc.utils import relpath

if TYPE_CHECKING:
    from dvc.path_info import PathInfo

logger = logging.getLogger(__name__)


class DulwichBackend(GitBackend):
    """Dulwich Git backend."""

    def __init__(self, root_dir: Union["PathInfo", str], **kwargs):
        from dulwich.repo import Repo

        super().__init__(root_dir)
        self.repo = Repo(self.root_dir)

    def close(self):
        self.repo.close()

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
        self, url: str, refspecs: Iterable[str], force: Optional[bool] = False
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
            except DivergedBranches as exc:
                raise SCMError("Experiment branch has diverged") from exc
            self.repo.refs[rh] = fetch_result.refs[lh]
