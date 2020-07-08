class Experiments:
    def __init__(self, repo):
        self.repo = repo

    def show(self, *args, **kwargs):
        from dvc.repo.experiments.show import show

        return show(self.repo, *args, **kwargs)

    def list(self, *args, **kwargs):
        pass
