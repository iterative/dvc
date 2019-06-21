import shutil


def destroy(self):
    for stage in self.stages():
        stage.remove(remove_outs=False)

    shutil.rmtree(self.dvc_dir)
