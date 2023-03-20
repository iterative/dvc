import builtins
import os
from time import sleep
from typing import List, Optional

from dvc.env import DVC_CHECKPOINT, DVC_ROOT
from dvc.repo import Repo
from dvc.stage.monitor import CheckpointTask


def make_checkpoint():
    """
    Signal DVC to create a checkpoint experiment.

    If the current process is being run from DVC, this function will block
    until DVC has finished creating the checkpoint. Otherwise, this function
    will return immediately.
    """
    if os.getenv(DVC_CHECKPOINT) is None:
        return

    root_dir = os.getenv(DVC_ROOT, Repo.find_root())
    signal_file = os.path.join(
        root_dir, Repo.DVC_DIR, "tmp", CheckpointTask.SIGNAL_FILE
    )

    with builtins.open(signal_file, "w", encoding="utf-8") as fobj:
        # NOTE: force flushing/writing empty file to disk, otherwise when
        # run in certain contexts (pytest) file may not actually be written
        fobj.write("")
        fobj.flush()
        os.fsync(fobj.fileno())
    while os.path.exists(signal_file):
        sleep(0.1)


def exp_save(
    name: Optional[str] = None,
    force: bool = False,
    include_untracked: Optional[List[str]] = None,
):
    """
    Create a new DVC experiment using `exp save`.

    See https://dvc.org/doc/command-reference/exp/save.

    Args:
        name (str, optional): specify a name for this experiment.
            If `None`, a default one will be generated, such as `urban-sign`.
            Defaults to `None`.
        force (bool):  overwrite the experiment if an experiment with the same
            name already exists.
            Defaults to `False`.
        include_untracked (List[str], optional): specify untracked file(s) to
            be included in the saved experiment.
            Defaults to `None`.

    Returns:
        str: The `Git revision`_ of the created experiment.

    Raises:
        ExperimentExistsError: If an experiment with `name` already exists and
            `force=False`.

    .. _Git revision:
        https://git-scm.com/docs/revisions
    """
    with Repo() as repo:
        return repo.experiments.save(
            name=name, force=force, include_untracked=include_untracked
        )
