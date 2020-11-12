class Vdir:
    def __init__(self, repo):
        self.repo = repo

    def pull(self, *args, **kwargs):
        from .pull import pull

        return pull(self.repo, *args, **kwargs)

    def cp(self, *args, **kwargs):
        from .cp import cp

        return cp(self.repo, *args, **kwargs)
