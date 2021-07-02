import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Iterable, Optional

from dvc.progress import Tqdm

from .errors import ObjectFormatError
from .tree import Tree

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo

    from .db.base import ObjectDB
    from .file import HashFile

logger = logging.getLogger(__name__)


def save(
    odb: "ObjectDB",
    obj: "HashFile",
    jobs: Optional[int] = None,
    **kwargs,
):
    assert obj.path_info and obj.fs and obj.hash_info

    if isinstance(obj, Tree):
        with ThreadPoolExecutor(max_workers=jobs) as executor:
            for future in Tqdm(
                as_completed(
                    executor.submit(
                        odb.add,
                        entry.path_info,
                        entry.fs,
                        entry.hash_info,
                        **kwargs,
                    )
                    for _, entry in obj
                ),
                desc="Saving files",
                total=len(obj),
                unit="file",
            ):
                future.result()

    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odbs: Iterable["ObjectDB"], obj: "HashFile") -> "ObjectDB":
    for odb in odbs:
        try:
            _check(odb, obj)
            return odb
        except (FileNotFoundError, ObjectFormatError):
            pass
    raise FileNotFoundError


def _check(odb: "ObjectDB", obj: "HashFile"):
    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)

    odb.check(obj.hash_info)


def load(odb: "ObjectDB", hash_info: "HashInfo") -> "HashFile":
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)
