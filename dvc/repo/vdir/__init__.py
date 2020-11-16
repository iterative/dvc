from dataclasses import dataclass

from dvc.hash_info import HashInfo


class Vdir:
    def __init__(self, repo):
        self.repo = repo

    def pull(self, *args, **kwargs):
        from .pull import pull

        return pull(self.repo, *args, **kwargs)

    def cp(self, *args, **kwargs):
        from .cp import cp

        return cp(self.repo, *args, **kwargs)


@dataclass
class VirtualDirData:
    operation: str  # e.g. cp, mv, rm
    src: str
    dst: str
    rm_paths: list

    local_src: bool = True  # only for cp
    hash_info: HashInfo = None
