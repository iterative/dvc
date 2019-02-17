import os


def add(self, fname, recursive=False, no_commit=False):
    from dvc.stage import Stage

    fnames = []
    if recursive and os.path.isdir(fname):
        for root, dirs, files in os.walk(str(fname)):
            for f in files:
                path = os.path.join(root, f)
                if Stage.is_stage_file(path):
                    continue
                if os.path.basename(path) == self.scm.ignore_file:
                    continue
                if self.scm.is_tracked(path):
                    continue
                fnames.append(path)
    else:
        fnames = [fname]

    stages = []
    self.files_to_git_add = []
    with self.state:
        for f in fnames:
            stage = Stage.create(repo=self, outs=[f], add=True)

            if stage is None:
                stages.append(stage)
                continue

            stage.save()
            if not no_commit:
                stage.commit()
            stages.append(stage)

    self.check_dag(self.stages() + stages)

    for stage in stages:
        if stage is not None:
            stage.dump()

    self.remind_to_git_add()

    return stages
