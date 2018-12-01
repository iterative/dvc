from dvc.command.base import CmdBase


class CmdDaemonBase(CmdBase):
    def __init__(self, args):
        self.args = args
        self.config = None
        self._set_loglevel(args)

    def run_cmd(self):
        return self.run()


class CmdDaemonUpdater(CmdDaemonBase):
    def run(self):
        import os
        from dvc.project import Project
        from dvc.updater import Updater

        root_dir = Project._find_root()
        dvc_dir = os.path.join(root_dir, Project.DVC_DIR)
        updater = Updater(dvc_dir)
        updater.fetch(detach=False)

        return 0


class CmdDaemonAnalytics(CmdDaemonBase):
    def run(self):
        from dvc.analytics import Analytics

        analytics = Analytics.load(self.args.target)
        analytics.send()

        return 0
