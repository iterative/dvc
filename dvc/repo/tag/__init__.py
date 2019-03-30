class Tag(object):
    def __init__(self, repo):
        self.repo = repo

    def add(self, *args, **kwargs):
        from dvc.repo.tag.add import add

        return add(self.repo, *args, **kwargs)

    def list(self, *args, **kwargs):
        from dvc.repo.tag.list import list

        return list(self.repo, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from dvc.repo.tag.remove import remove

        return remove(self.repo, *args, **kwargs)
