from dvc.utils import resolve_paths

from . import locked


@locked
def export_to_remote(self, source, destination, remote=None, jobs=None):
    from dvc.dvcfile import Dvcfile
    from dvc.stage import Stage, create_stage
    from dvc.tree import get_cloud_tree

    from_tree = get_cloud_tree(self, url=source)
    from_tree.config["jobs"] = jobs

    remote_tree = self.cloud.get_remote(remote).tree

    hash_info = from_tree.export(
        remote_tree, from_tree.path_info, remote_tree.path_info, repo=self,
    )

    path, _, _ = resolve_paths(self, destination)
    stage = create_stage(Stage, self, path, outs=[destination])

    dvcfile = Dvcfile(self, stage.path)
    dvcfile.remove()

    stage.outs[0].hash_info = hash_info
    dvcfile.dump(stage)
    return hash_info.nfiles
