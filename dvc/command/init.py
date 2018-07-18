from dvc.logger import Logger


class CmdInit(object):
    def __init__(self, args):
        self.args = args

    def run_cmd(self):
        from dvc.project import Project, InitError

        try:
            Project.init('.', no_scm=self.args.no_scm, force=self.args.force)
        except InitError as e:
            Logger.error('Failed to initiate dvc', e)
            return 1
        return 0
