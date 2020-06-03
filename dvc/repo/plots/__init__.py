class Plots:
    def __init__(self, repo):
        self.repo = repo

    def show(self, *args, **kwargs):
        from .show import show

        return show(self.repo, *args, **kwargs)

    def diff(self, *args, **kwargs):
        from .diff import diff

        return diff(self.repo, *args, **kwargs)

    def modify(self, path, props=None, unset=None):
        from dvc.dvcfile import Dvcfile

        (out,) = self.repo.find_outs_by_path(path)

        # This out will become a plot unless it is one already
        if not isinstance(out.plot, dict):
            out.plot = {}

        for field in unset or ():
            out.plot.pop(field, None)
        out.plot.update(props or {})

        # Empty dict will move it to non-plots
        if not out.plot:
            out.plot = True

        out.verify_metric()

        dvcfile = Dvcfile(self.repo, out.stage.path)
        dvcfile.dump(out.stage, update_pipeline=True)
