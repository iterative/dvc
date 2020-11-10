from ..tree import get_cloud_tree
from .base import Remote
from .local import LocalRemote


def get_remote(repo, **kwargs):
    tree = get_cloud_tree(repo, **kwargs)
    if tree.scheme == "local":
        return LocalRemote(tree)
    return Remote(tree)
