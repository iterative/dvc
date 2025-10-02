from typing import TYPE_CHECKING, Optional

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
            # skip strings or unknown types
            logger.debug("Skipping non-stage item in collect(): %r", item)
    return outs


@locked
def purge(
    self: "Repo",
    targets: Optional[list[str]] = None,
    recursive: bool = False,
    force: bool = False,
    dry_run: bool = False,
):
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
        if targets:
            items = collect(self, targets=targets, recursive=recursive)
        else:
            items = list(self.index.stages)
    except StageFileDoesNotExistError as e:
        raise PurgeError(str(e)) from e

    outs = _flatten_stages_or_outs(items)

    if not outs:
        logger.info("No DVC-tracked outputs found to purge.")
        return 0

    # --- SAFETY CHECK 1: dirty outs
    dirty = [o for o in outs if o.use_cache and o.changed()]
    if dirty and not force:
        raise PurgeError(
            "Some tracked outputs have uncommitted changes. "
            "Use `--force` to purge anyway."
        )

    # --- SAFETY CHECK 2: remote + remote presence
    not_in_remote = []
    try:
        remote_odb = self.cloud.get_remote_odb(None)  # default remote
    except Exception:
        remote_odb = None

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
    else:
        # remote exists, check objects
        for out in outs:
            if out.use_cache and out.hash_info and out.hash_info.value:
                if not remote_odb.exists(out.hash_info.value):
                    not_in_remote.append(str(out))

        if not_in_remote:
            if not force:
                raise PurgeError(
                    "Some outputs are not present in the remote cache and would be "
                    "permanently lost if purged:\n  - "
                    + "\n  - ".join(not_in_remote)
                    + "\nUse `--force` to purge anyway."
                )
            logger.warning(
                "Some outputs are not present in the remote cache and may be "
                "permanently lost:\n  - " + "\n  - ".join(not_in_remote)
            )

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
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to remove %s: %s", out, e)

    if removed:
        logger.info("Removed %d outputs (workspace + cache).", removed)
    else:
        logger.info("Nothing to purge.")

    return 0
