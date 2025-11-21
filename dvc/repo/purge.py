from typing import TYPE_CHECKING, Optional

from dvc.config import NoRemoteError, RemoteNotFoundError
from dvc.exceptions import DvcException
from dvc.log import logger

from . import locked

if TYPE_CHECKING:
    from dvc.output import Output
    from dvc.repo import Repo

logger = logger.getChild(__name__)


class PurgeError(DvcException):
    """Raised when purge fails due to safety or internal errors."""


def _flatten_stages_or_outs(items) -> list["Output"]:
    """Normalize collect() results into a flat list of Output objects."""
    outs = []
    for item in items:
        if isinstance(item, list):
            outs.extend(_flatten_stages_or_outs(item))
        elif hasattr(item, "outs"):  # Stage
            outs.extend(item.outs)
        elif hasattr(item, "use_cache"):  # Already an Output
            outs.append(item)
        else:
            logger.debug("Skipping non-stage item in collect(): %r", item)
    return outs


def _check_dirty(outs, force: bool) -> None:
    dirty = [o for o in outs if o.use_cache and o.changed()]
    if dirty and not force:
        raise PurgeError(
            "Some tracked outputs have uncommitted changes. "
            "Use `--force` to purge anyway.\n  - "
            + "\n  - ".join(str(o) for o in dirty)
        )


def _get_remote_odb(repo: "Repo"):
    try:
        return repo.cloud.get_remote_odb(None)
    except (RemoteNotFoundError, NoRemoteError):
        return None


def _check_remote_backup(repo: "Repo", outs, force: bool) -> None:
    remote_odb = _get_remote_odb(repo)

    if not remote_odb:
        if not force:
            raise PurgeError(
                "No default remote configured. "
                "Cannot safely purge outputs without verifying remote backup.\n"
                "Use `--force` to purge anyway."
            )
        logger.warning(
            "No default remote configured. Proceeding with purge due to --force. "
            "Outputs may be permanently lost."
        )
        return

    # remote exists, check objects
    not_in_remote = [
        str(o)
        for o in outs
        if o.use_cache
        and o.hash_info
        and o.hash_info.value
        and not remote_odb.exists(o.hash_info.value)
    ]
    if not_in_remote and not force:
        raise PurgeError(
            "Some outputs are not present in the remote cache and would be "
            "permanently lost if purged:\n  - "
            + "\n  - ".join(not_in_remote)
            + "\nUse `--force` to purge anyway."
        )
    if not_in_remote and force:
        logger.warning(
            "Some outputs are not present in the remote cache and may be "
            "permanently lost:\n  - %s",
            "\n  - ".join(not_in_remote),
        )


def _remove_outs(outs, dry_run: bool) -> int:
    removed = 0
    for out in outs:
        if dry_run:
            logger.info("[dry-run] Would remove %s", out)
            continue

        try:
            # remove workspace file
            if out.exists:
                out.remove(ignore_remove=False)

            # remove cache entry
            if out.use_cache and out.hash_info:
                cache_path = out.cache.oid_to_path(out.hash_info.value)
                if out.cache.fs.exists(cache_path):
                    out.cache.fs.remove(cache_path, recursive=True)

            removed += 1
        except Exception:
            logger.exception("Failed to remove %s", out)
    return removed


@locked
def purge(
    self: "Repo",
    targets: Optional[list[str]] = None,
    recursive: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> int:
    """
    Purge removes local copies of DVC-tracked outputs and their cache.

    - Collects outs from .dvc files and dvc.yaml.
    - Ensures safety (no dirty outs unless --force).
    - Ensures outputs are backed up to remote (unless --force).
    - Removes both workspace copies and cache objects.
    - Metadata remains intact.
    """
    from dvc.repo.collect import collect
    from dvc.stage.exceptions import StageFileDoesNotExistError

    try:
        items = (
            collect(self, targets=targets, recursive=recursive)
            if targets
            else list(self.index.stages)
        )
    except StageFileDoesNotExistError as e:
        raise PurgeError(str(e)) from e

    outs = _flatten_stages_or_outs(items)
    if not outs:
        logger.info("No DVC-tracked outputs found to purge.")
        return 0

    # Run safety checks
    _check_dirty(outs, force)
    _check_remote_backup(self, outs, force)

    # Remove outs
    removed = _remove_outs(outs, dry_run)

    if removed:
        logger.info("Removed %d outputs (workspace + cache).", removed)
    else:
        logger.info("Nothing to purge.")
    return 0
