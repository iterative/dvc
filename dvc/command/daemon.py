from dvc.command.base import CmdBase


class CmdDaemonUpdater(CmdBase):
    def __init__(self, args):
        pass

    def run_cmd(self):
        return self.run()

    def run(self):
        import os
        from dvc.project import Project
        from dvc.updater import Updater

        root_dir = self._find_root()
        dvc_dir = os.path.join(root_dir, Project.DVC_DIR)
        updater = Updater(dvc_dir)
        updater.fetch(detach=False)
