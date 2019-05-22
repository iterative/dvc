class Pkg(object):
    def __init__(self, repo):
        self.repo = repo

    def install(self, *args, **kwargs):
        from dvc.repo.pkg.install import install

        return install(self.repo, *args, **kwargs)
