from dvc.utils.fs import remove


def destroy(self):
    for stage in self.stages:
        stage.remove(remove_outs=False)

    remove(self.dvc_dir)
