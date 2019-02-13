import shutil


def destroy(self):
    for stage in self.stages():
        stage.remove()

    shutil.rmtree(self.dvc_dir)
