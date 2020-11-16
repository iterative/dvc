from glob import glob
from shutil import copy

from dvc.path_info import PosixPathInfo
from dvc.repo.add import add

from . import VirtualDirData


def cp(repo, src=None, dst=None, local_src=False):

    if local_src:
        copy(src, dst)

    vdir = VirtualDirData(
        operation="cp", src=src, dst=dst, rm_paths=None, local_src=local_src
    )
    target = _derive_target(dst)

    return add(repo, target, vdir=vdir)


# TODO: find similar util func in dvc
def _derive_target(dst):
    for p in PosixPathInfo(dst).parts:
        for g in glob("*.dvc"):
            if p in g:
                return p
