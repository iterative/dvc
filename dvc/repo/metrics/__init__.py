class Metrics(object):
    def __init__(self, repo):
        self.repo = repo

    def add(self, *args, **kwargs):
        from dvc.repo.metrics.add import add

        return add(self.repo, *args, **kwargs)

    def modify(self, *args, **kwargs):
        from dvc.repo.metrics.modify import modify

        return modify(self.repo, *args, **kwargs)

    def show(self, *args, **kwargs):
        from dvc.repo.metrics.show import show

        return show(self.repo, *args, **kwargs)

    def remove(self, *args, **kwargs):
        from dvc.repo.metrics.remove import remove

        return remove(self.repo, *args, **kwargs)
