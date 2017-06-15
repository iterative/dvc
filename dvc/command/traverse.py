import os

from dvc.command.base import CmdBase, DvcLock
from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.data_cloud import DataCloud


class TraverseError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Traverse file tree error: {}'.format(msg))


class Traverse(CmdBase):
    def __init__(self, settings, cmd_name, do_not_start_from_root=True):
        super(Traverse, self).__init__(settings)
        self._cmd_name = cmd_name
        self.cloud = DataCloud(self.settings)
        self._do_not_start_from_root = do_not_start_from_root

    def define_args(self, parser):
        self.set_no_git_actions(parser)

        parser.add_argument('target', metavar='', help='Target to remove - file or directory.', nargs='*')
        # parser.add_argument('-r', '--recursive', action='store_true', help='CmdGarbage collect directory recursively.')
        parser.add_argument('-l', '--keep-in-cloud', action='store_true', default=False,
                            help='Do not remove data from cloud.')
        pass

    def run(self):
        with DvcLock(self.is_locker, self.git):
            if not self._traverse_all():
                return 1

        return 0

    def _traverse_all(self):
        if not self.no_git_actions and not self.git.is_ready_to_go():
            return False

        error = False
        print('TARGET={}'.format(self.parsed_args.target))
        for target in self.parsed_args.target:
            if not self._traverse(target):
                error = True

        message = 'DVC {}: {}'.format(self._cmd_name, ' '.join(self.parsed_args.target))
        self.commit_if_needed(message, error)

        return error == 0

    def _traverse(self, target):
        try:
            if os.path.isdir(target):
                if not self.is_recursive:
                    msg = '[TraverseFileTree] Directory "%s" cannot be traversed. Use --recurcive flag.'
                    raise TraverseError(msg % target)

                if self._do_not_start_from_root and self.settings.path_factory.data_item(target).data_dvc_short == '':
                    # The entire data directory
                    raise TraverseError('[Traverse] Root data directory "%s" cannot be traversed' % target)

                self._traverse_dir(target)
            else:
                self.process_file(target)
            return True
        except DvcException as ex:
            Logger.error('[TraverseFileTree] Unable to {} data item "{}": {}'.format(self._cmd_name, target, ex))
            return False

    def _traverse_dir(self, target):
        for f in os.listdir(target):
            file = os.path.join(target, f)
            if os.path.isdir(file):
                self._traverse_dir(file)
            else:
                self.process_file(file)

        self.traverse_dir_finalize(target)
        pass

    ## API:
    def is_recursive(self):
        return True

    def process_file(self, target):
        pass

    def traverse_dir_finalize(self, target):
        pass
