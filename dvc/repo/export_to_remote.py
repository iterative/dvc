from dvc.utils import resolve_paths

from . import locked


@locked
def export_to_remote(self, source, destination, remote=None, jobs=None):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage

    hash_info = self.cloud.transfer(source, jobs=jobs, remote=remote)

    path, _, _ = resolve_paths(self, destination)
    stage = create_stage(Stage, self, path, outs=[destination])

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove()

    stage.outs[0].hash_info = hash_info
    dvcfile.dump(stage)
    return hash_info.nfiles
