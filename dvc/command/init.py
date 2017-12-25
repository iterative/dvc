from dvc.project import Project


class CmdInit(object):
    def __init__(self, args):
        self.args = args

    def run_cmd(self):
        Project.init('.')
        return 0
