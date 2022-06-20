import builtins
import os
from time import sleep

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
