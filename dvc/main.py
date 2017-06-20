from __future__ import print_function

from dvc.command.import_file import CmdImportFile

"""
main entry point / argument parsing for dvc
"""

import sys

from dvc.runtime import Runtime
from dvc.command.init import CmdInit
from dvc.command.remove import CmdRemove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataSync
from dvc.command.lock import CmdLock
from dvc.command.test import CmdTest

VERSION = '0.8.5'

def print_usage():
    usage = ('',
            'usage: dvc [--version] [--help] command [<args>]',
            '',
            'These are common DVC commands:',
            '',
            'start a working area',
            '    init           Initialize dvc over a directory (should already be a git dir).',
            '    run            Run command.',
            '    import         Import file to data directory.',
            '    remove         Remove data item from data directory.',
            '',
            'synchronize data between remote and local',
            '    data sync      Synchronize data file with cloud (cloud settings already setup).',
)
    print('\n'.join(usage))

def main():
    cmds = ['--help', '--version', 'init', 'run', 'sync', 'repro', 'data', 'remove', 'import', 'lock', 'cloud', \
            'cloud', 'cloud-run', 'cloud-instance-create', 'cloud-instance-remove', 'cloud-instance-describe', \
            'test', 'test-aws', 'test-gcloud', 'test-cloud']

    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        if len(sys.argv) >= 2:
            print('Unimplemented or unrecognized command: ' + ' '.join(sys.argv[1:]))
        print_usage()
        sys.exit(-1)

    cmd = sys.argv[1]

    if cmd == '--help':
        print_usage()
    elif cmd == '--version':
        print('dvc version {}'.format(VERSION))
    elif cmd == 'init':
        Runtime.run(CmdInit, parse_config=False)
    elif cmd == 'run':
        Runtime.run(CmdRun)
    elif cmd == 'repro':
        Runtime.run(CmdRepro)
    elif cmd == 'sync':
        Runtime.run(CmdDataSync)
    elif cmd == 'import':
        Runtime.run(CmdImportFile)
    elif cmd == 'remove':
        Runtime.run(CmdRemove)
    elif cmd == 'lock':
        Runtime.run(CmdLock)
    elif cmd == 'cloud-run':
        print('cloud-run unimplemented')
    elif cmd == 'cloud-instance-create':
        print('cloud-instance-create unimplemented')
    elif cmd == 'clould-instance-remove':
        print('cloud-instance-remove unimplemented')
    elif cmd == 'cloud-instance-describe':
        print('cloud-instance-describe unimplemented')

    elif cmd == 'test-aws':
        print('TODO: test aws credentials')
    elif cmd == 'test-gcloud':
        Runtime.run(CmdTest)
    else:
        print('Unimplemented or unrecognized command. ' + ' '.join(sys.argv[1]))
        print_usage()
        sys.exit(-1)
