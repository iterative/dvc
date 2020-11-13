from ..tree import get_cloud_tree
from .base import Remote
from .ipfs import IPFSRemote
from .local import LocalRemote
from .ssh import SSHRemote


def get_remote(repo, **kwargs):
    tree = get_cloud_tree(repo, **kwargs)
    if tree.scheme == "local":
        return LocalRemote(tree)
    if tree.scheme == "ssh":
        return SSHRemote(tree)
    if tree.scheme == "ipfs":
        return IPFSRemote(tree)
    return Remote(tree)
