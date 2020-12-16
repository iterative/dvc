import logging
import os
import sys
from tempfile import TemporaryDirectory
from typing import Optional

from dvc.utils.fs import remove

from .base import BaseExecutor

logger = logging.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Local machine experiment executor."""

    def __init__(
        self,
        *args,
        tmp_dir: Optional[str] = None,
        cache_dir: Optional[str] = None,
        **kwargs,
    ):
        self._tmp_dir = TemporaryDirectory(dir=tmp_dir)
        kwargs["root_dir"] = self._tmp_dir.name
        super().__init__(*args, **kwargs)
        if cache_dir:
            self._config(cache_dir)
        logger.debug(
            "Init local executor in dir '%s'", self._tmp_dir,
        )

    def _config(self, cache_dir):
        local_config = os.path.join(self.dvc_dir, "config.local")
        logger.debug("Writing experiments local config '%s'", local_config)
        with open(local_config, "w") as fobj:
            fobj.write(f"[cache]\n    dir = {cache_dir}")

    @property
    def git_url(self) -> str:
        root_dir = os.path.abspath(self.root_dir)
        if os.name == "nt":
            root_dir = root_dir.replace(os.sep, "/")
        return f"file://{root_dir}"

    def cleanup(self):
        super().cleanup()
        logger.debug("Removing tmpdir '%s'", self._tmp_dir)
        try:
            self._tmp_dir.cleanup()
        except PermissionError:
            if os.name == "nt" and sys.version_info < (3, 8):
                # see https://bugs.python.org/issue26660
                remove(self._tmp_dir.name)
                return
            raise
