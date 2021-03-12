import logging

from dvc.progress import Tqdm

from .tree import Tree

logger = logging.getLogger(__name__)


def save(odb, obj, **kwargs):
    if isinstance(obj, Tree):
        for _, entry in Tqdm(obj):
            odb.add(entry.path_info, entry.fs, entry.hash_info, **kwargs)
    odb.add(obj.path_info, obj.fs, obj.hash_info, **kwargs)


def check(odb, obj):
    odb.check(obj.hash_info)

    if isinstance(obj, Tree):
        for _, entry in obj:
            odb.check(entry.hash_info)


def load(odb, hash_info):
    if hash_info.isdir:
        return Tree.load(odb, hash_info)
    return odb.get(hash_info)
