class Vdir:
    def __init__(self, repo):
        self.repo = repo

    def pull(self, *args, **kwargs):
        from .pull import pull

        return pull(self.repo, *args, **kwargs)

    def add(self, *args, **kwargs):
        from .add import add

        return add(self.repo, *args, **kwargs)
