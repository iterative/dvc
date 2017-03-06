import sys
import os
from pathlib import Path
from shutil import copyfile

from neatlynx.base_cmd import BaseCmd, Logger
from neatlynx.data_file_obj import DataFileObj
from neatlynx.exceptions import NeatLynxException
from neatlynx.git_wrapper import GitWrapper


class DataImportError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Import error: {}'.format(msg))


class DataImport(BaseCmd):
    def __init__(self):
        BaseCmd.__init__(self)
        pass

    def define_args(self, parser):
        self.add_string_arg(parser, 'input', 'Input file')
        self.add_string_arg(parser, 'output', 'Output file')
        pass

    def run(self):
        input = Path(self.args.input)
        if not input.exists():
            raise DataImportError('Input file "{}" does not exist'.format(input))
        if not input.is_file():
            raise DataImportError('Input file "{}" has to be a regular file'.format(input))

        dataFileObj = DataFileObj(self.args.output, self._config, GitWrapper.curr_commit())

        #output = Path(self.args.output)
        if dataFileObj.data_file_relative.exists():
            raise DataImportError('Output file "{}" already exists'.format(dataFileObj.data_file_relative))
        if os.path.isdir(dataFileObj.data_dir_relative):
            raise DataImportError('Output file directory "{}" does not exists'.format(dataFileObj.data_dir_relative))

        data_dir_path = Path(self.config.DataDir)
        if output.parent < data_dir_path:
            raise DataImportError('Output file has to be in data dir - {}'.format(data_dir_path))

        # data_dir_path_str = str(data_dir_path)
        # output_dir_str = str(output.parent)
        # relative_dir = output_dir_str[len(data_dir_path_str):].strip(os.path.sep)
        #
        # cache_file_dir = os.path.join(self.config.CachDir, relative_dir)
        # cache_file_dir_path = Path(cache_file_dir)
        #
        # state_file_dir = os.path.join(self.config.StateDir, relative_dir)
        # state_file_dir_path = Path(state_file_dir)
        #
        # commit = GitWrapper.curr_commit()
        # cache_file_name = output.name + '_' + commit
        # cache_file = cache_file_dir_path / cache_file_name
        # state_file = state_file_dir_path / output.name

        # Perform actions
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.parent.mkdir(parents=True, exist_ok=True)

        copyfile(self.args.input, str(cache_file))
        Logger.verbose('Input file "{}" was copied to cache "{}"'.format(self.args.input, cache_file))

        output.symlink_to(cache_file)
        Logger.verbose('Symlink from data file "{}" the cache file "{}" was created'.
                       format(output, cache_file))

        StateFile.create(state_file, input, output.absolute(), cache_file.absolute())
        pass


if __name__ == '__main__':
    try:
        sys.exit(DataImport().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
