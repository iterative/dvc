from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.runtime import Runtime
from dvc.state_file import StateFile


class CmdLock(CmdBase):
    def __init__(self, settings):
        super(CmdLock, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)

        parser.add_argument('-u', '--unlock', action='store_true', default=False,
                            help='Unlock data item - enable reproduction.')

        parser.add_argument('files', metavar='', help='Data items to lock or unlock.', nargs='*')
        pass

    def run(self):
        with DvcLock(self.is_locker, self.git):
            return self.lock_files(self.parsed_args.files, not self.parsed_args.unlock)

    def lock_files(self, files, target):
        cmd = 'lock' if target else 'unlock'

        error = 0
        for file in files:
            try:
                data_item = self.settings.path_factory.existing_data_item(file)
                state = StateFile.load(data_item.state.relative, self.settings)

                if state.locked and target:
                    Logger.warn('Data item {} is already locked'.format(data_item.data.relative))
                elif not state.locked and not target:
                    Logger.warn('Data item {} is already unlocked'.format(data_item.data.relative))
                else:
                    state.locked = target
                    Logger.debug('Saving status file for data item {}'.format(data_item.data.relative))
                    state.save()
                    Logger.info('Data item {} was {}ed'.format(data_item.data.relative, cmd))
            except Exception as ex:
                error += 1
                Logger.error('Unable to {} {}: {}'.format(cmd, file, ex))

        if error > 0 and not self.no_git_actions:
            Logger.error('Errors occurred. One or more repro cmd was not successful.')
            self.not_committed_changes_warning()
        else:
            self.commit_if_needed('DVC lock: {}'.format(' '.join(self.args)))

        return 0


if __name__ == '__main__':
    Runtime.run(CmdLock, False)
