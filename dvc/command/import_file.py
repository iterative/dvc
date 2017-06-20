import os
from shutil import copyfile
import re
import requests
from multiprocessing.pool import ThreadPool

from dvc.command.base import CmdBase, DvcLock
from dvc.data_cloud import sizeof_fmt
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.runtime import Runtime
from dvc.state_file import StateFile
from dvc.system import System
from dvc.command.data_sync import POOL_SIZE
from dvc.progress import Progress


class ImportFileError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Import file: {}'.format(msg))


class CmdImportFile(CmdBase):
    def __init__(self, settings):
        super(CmdImportFile, self).__init__(settings)

    def define_args(self, parser):
        self.set_no_git_actions(parser)
        self.set_lock_action(parser)

        parser.add_argument('input',
                            help='Input file/files.',
                            nargs='+')
        parser.add_argument('output',
                            help='Output file/directory.')

    def run(self):
        targets = []
        with DvcLock(self.is_locker, self.git):
            output = self.parsed_args.output
            self.verify_output(output, self.parsed_args.input)

            for input in self.parsed_args.input:
                if not os.path.isdir(input):
                    targets.append((input, output))
                else:
                    input_dir = os.path.basename(input)
                    for root, dirs, files in os.walk(input):
                        for file in files:
                            filename = os.path.join(root, file)

                            rel = os.path.relpath(filename, input)
                            out = os.path.join(output, input_dir, rel)

                            out_dir = os.path.dirname(out)
                            if not os.path.exists(out_dir):
                                os.mkdir(out_dir)

                            targets.append((filename, out))
                pass
            self.import_files(targets, self.parsed_args.lock)
            message = 'DVC import files: {} -> {}'.format(str(self.parsed_args.input), output)
            self.commit_if_needed(message)
        pass

    @staticmethod
    def verify_output(output, input):
        if CmdImportFile.is_dir_path(output) and not os.path.isdir(output):
            raise ImportFileError(u'output directory {} does not exist'.format(output))

        if len(input) > 1 and not os.path.isdir(output):
            msg = u'output {} has to be directory for multiple file import'
            raise ImportFileError(msg.format(output))
        pass

    @staticmethod
    def is_dir_path(output):
        return len(output) > 0 and output[-1] == os.path.sep

    def import_and_commit_if_needed(self, input, output, lock=False, check_if_ready=True):
        if check_if_ready and not self.no_git_actions and not self.git.is_ready_to_go():
            return 1

        self.import_file(input, output, lock)

        message = 'DVC import file: {} {}'.format(input, output)
        return self.commit_if_needed(message)

    def import_file(self, input, output, lock):
        return self.import_files([(input, output)], lock)

    def collect_targets(self, targets):
        """
        Collect input, output and data_item's into tuples
        to be able to download them simulteniously later.
        """
        data_targets = []

        for target in targets:
            input = target[0]
            output = target[1]
            if not CmdImportFile.is_url(input):
                if not os.path.exists(input):
                    raise ImportFileError('Input file "{}" does not exist'.format(input))
                if not os.path.isfile(input):
                    raise ImportFileError('Input file "{}" has to be a regular file'.format(input))

            if os.path.isdir(output):
                output = os.path.join(output, os.path.basename(input))

            data_item = self.settings.path_factory.data_item(output)

            if os.path.exists(data_item.data.relative):
                raise ImportFileError('Output file "{}" already exists'.format(data_item.data.relative))
            if not os.path.isdir(os.path.dirname(data_item.data.relative)):
                raise ImportFileError('Output file directory "{}" does not exists'.format(
                    os.path.dirname(data_item.data.relative)))

            cache_dir = os.path.dirname(data_item.cache.relative)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir)

            data_targets.append((input, output, data_item))

        return data_targets

    def import_files(self, targets, lock=False):
        data_targets = self.collect_targets(targets)
        self.download_targets(data_targets)
        self.create_state_files(data_targets, lock)

    def download_target(self, target):
        """
        Download single target from url or from local path.
        """
        input = target[0]
        output = target[2].cache.relative

        if self.is_url(input):
            Logger.debug("Downloading {} -> {}.".format(input, output))
            self.download_file(input, output)
            Logger.debug("Done downloading {} -> {}.".format(input, output))
        else:
            Logger.debug("Copying {} -> {}".format(input, output))
            self.copy_file(input, output)
            Logger.debug("Dony copying {} -> {}".format(input, output))

    @staticmethod
    def copy_file(input, output):
        """
        Copy single file from local path.
        """
        copyfile(input, output)

    def download_file(self, from_url, to_file):
        """
        Download single file from url.
        """
        r = requests.get(from_url, stream=True)

        name = os.path.basename(from_url)
        chunk_size = 1024 * 100
        downloaded = 0
        last_reported = 0
        report_bucket = 100*1024*10
        total_length = r.headers.get('content-length')

        with open(to_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:  # filter out keep-alive new chunks
                    continue

                downloaded += chunk_size

                last_reported += chunk_size
                if last_reported >= report_bucket:
                    last_reported = 0
                    Logger.debug('Downloaded {}'.format(sizeof_fmt(downloaded)))

                # update progress bar
                self.progress.update_target(name, downloaded, total_length)

                f.write(chunk)

        # tell progress bar that this target is finished downloading
        self.progress.finish_target(name)

    def download_targets(self, targets):
        """
        Download targets in a number of threads.
        """
        self.progress = Progress(len(targets))
        p = ThreadPool(processes=POOL_SIZE)
        p.map(self.download_target, targets)
        self.progress.finish()

    def create_state_files(self, targets, lock):
        """
        Create state files for all targets.
        """
        for t in targets:
            input       = t[0]
            output      = t[1]
            data_item   = t[2]

            Logger.debug('Creating symlink {} --> {}'.format(data_item.symlink_file, data_item.data.relative))
            System.symlink(data_item.symlink_file, data_item.data.relative)

            state_file = StateFile(StateFile.COMMAND_IMPORT_FILE,
                               data_item.state.relative,
                               self.settings,
                               argv=[input, output],
                               input_files=[],
                               output_files=[output],
                               lock=lock)
            state_file.save()
            Logger.debug('State file "{}" was created'.format(data_item.state.relative))

    URL_REGEX = re.compile(
        r'^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    @staticmethod
    def is_url(url):
        return CmdImportFile.URL_REGEX.match(url) is not None


if __name__ == '__main__':
    Runtime.run(CmdImportFile)
