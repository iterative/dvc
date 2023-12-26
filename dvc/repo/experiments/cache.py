import os
from typing import TYPE_CHECKING, Optional, Union

from dvc.fs import localfs
from dvc.log import logger
from dvc_objects.db import ObjectDB

from .serialize import DeserializeError, SerializableError, SerializableExp
from .utils import EXEC_TMP_DIR

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logger.getChild(__name__)


class ExpCache:
    """Serialized experiment state cache.

    ODB with git SHAs as keys. Objects can be either SerializableExp or
    SerializableError.
    """

    CACHE_DIR = os.path.join(EXEC_TMP_DIR, "cache")

    def __init__(self, repo: "Repo"):
        path = os.path.join(repo.tmp_dir, self.CACHE_DIR)
        self.odb = ObjectDB(localfs, path)

    def delete(self, rev: str):
        self.odb.delete(rev)

    def put(
        self,
        exp: Union[SerializableExp, SerializableError],
        rev: Optional[str] = None,
        force: bool = False,
    ):
        rev = rev or getattr(exp, "rev", None)
        assert rev
        assert rev != "workspace"
        if force or not self.odb.exists(rev):
            try:
                self.delete(rev)
            except FileNotFoundError:
                pass
            self.odb.add_bytes(rev, exp.as_bytes())
            logger.trace("ExpCache: cache put '%s'", rev[:7])

    def get(self, rev: str) -> Optional[Union[SerializableExp, SerializableError]]:
        obj = self.odb.get(rev)
        try:
            with obj.fs.open(obj.path, "rb") as fobj:
                data = fobj.read()
        except FileNotFoundError:
            logger.trace("ExpCache: cache miss '%s'", rev[:7])
            return None
        for typ in (SerializableExp, SerializableError):
            try:
                exp = typ.from_bytes(data)  # type: ignore[attr-defined]
                logger.trace("ExpCache: cache load '%s'", rev[:7])
                return exp
            except DeserializeError:
                continue
        logger.debug("ExpCache: unknown object type for '%s'", rev)
        return None
