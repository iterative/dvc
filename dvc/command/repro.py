from dvc.command.common.base import CmdBase

class CmdRepro(CmdBase):
    def run(self):
        recursive = not self.args.single_item
        self.project.reproduce(self.args.targets,
                               recursive=recursive,
                               force=self.args.force)
