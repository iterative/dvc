import dvc.logger as logger


class CmdInit(object):
    def __init__(self, args):
        self.args = args

    def run_cmd(self):
        from dvc.project import Project, InitError

        try:
            self.project = Project.init('.',
                                        no_scm=self.args.no_scm,
                                        force=self.args.force)
            self.config = self.project.config
        except InitError:
            logger.error('failed to initiate dvc')
            return 1
        return 0
