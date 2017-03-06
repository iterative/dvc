import sys
import os
from pathlib import Path
from shutil import copyfile

from neatlynx.cmd_base import CmdBase, Logger
from neatlynx.data_file_obj import DataFileObj
from neatlynx.exceptions import NeatLynxException


class DataImportError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Import error: {}'.format(msg))


class CmdDataImport(CmdBase):
    def __init__(self):
        CmdBase.__init__(self)
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

        output = self.args.output
        if os.path.isdir(self.args.output):
            output = os.path.join(output, os.path.basename(self.args.input))

        dobj = DataFileObj(output, self.git, self.config)

        if os.path.exists(dobj.data_file_relative):
            raise DataImportError('Output file "{}" already exists'.format(dobj.data_file_relative))
        if not os.path.isdir(os.path.dirname(dobj.data_file_abs)):
            raise DataImportError('Output file directory "{}" does not exists'.format(
                os.path.dirname(dobj.data_file_relative)))

        os.makedirs(os.path.dirname(dobj.cache_file_relative), exist_ok=True)
        copyfile(self.args.input, dobj.cache_file_relative)
        Logger.verbose('Input file "{}" was copied to cache "{}"'.format(
            self.args.input, dobj.cache_file_relative))

        cache_relative_to_data = os.path.relpath(dobj.cache_file_relative, os.path.dirname(dobj.data_file_relative))
        os.symlink(cache_relative_to_data, dobj.data_file_relative)
        Logger.verbose('Symlink from data file "{}" to the cache file "{}" was created'.
                       format(dobj.data_file_relative, cache_relative_to_data))

        os.makedirs(os.path.dirname(dobj.state_file_relative), exist_ok=True)
        with open(dobj.state_file_relative, 'w') as fd:
            fd.write('NLX_state. v0.1\n')
            fd.write('Args: {}\n'.format(sys.argv))
        Logger.verbose('State file "{}" was created'.format(dobj.state_file_relative))
        pass


if __name__ == '__main__':
    try:
        sys.exit(CmdDataImport().run())
    except NeatLynxException as e:
        Logger.error(e)
        sys.exit(1)
