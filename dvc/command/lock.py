from dvc.command.common.base import CmdBase
from dvc.stage import Stage


class CmdLock(CmdBase):
    def run(self):
        lock = not self.args.unlock
        cmd = 'lock' if lock else 'unlock'
        ret = 0
        for file in self.args.files:
            try:
                stage = Stage.load(self.project, file)

                if stage.locked and lock:
                    self.project.logger.warn('Stage {} is already locked'.format(file))
                elif not stage.locked and not lock:
                    self.project.logger.warn('Stage {} is already unlocked'.format(file))
                else:
                    stage.locked = lock
                    self.project.logger.debug('Saving stage file {}'.format(file))
                    stage.dump()
                    self.project.logger.info('Stage {} was {}ed'.format(file, cmd))
            except Exception as ex:
                ret = 1
                self.project.logger.error('Unable to {} {}: {}'.format(cmd, file, ex))

        return ret
