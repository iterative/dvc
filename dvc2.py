from __future__ import print_function

from dvc.command.import_file import CmdImportFile

"""
main entry point / argument parsing for dvc

NB: for ${REASONS}, cannot be named dvc.py.  trust me.
"""

import sys

from dvc.runtime import Runtime
from dvc.command.init import CmdInit
from dvc.command.import_bulk import CmdImportBulk
from dvc.command.remove import CmdDataRemove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataSync
from dvc.command.import_bulk import CmdImportBulk
from dvc.command.lock import CmdLock
from dvc.command.test import CmdTest


def print_usage():
    usage = ('',
            'These are common dvc commands:',
            '',
            'start a working area',
            '    init    initialize dvc control over a dir (should already be a git dir)',
            '    run TODO',
            '    sync TODO',
            '',
            'synchronize data between remote and local',
            '    data sync    TODO',
            '    data remove  TODO',
            '    data import  TODO',
            '',
            'test credentials',
            '    test aws TODO',
            '    test gcloud  test gcloud and credentials are setup correctly',
            '')
    print('\n'.join(usage))

if __name__ == '__main__':
    cmds = ['init', 'run', 'sync', 'repro', 'data', 'remove', 'import', 'lock', 'cloud', \
            'cloud', 'cloud-run', 'cloud-instance-create', 'cloud-instance-remove', 'cloud-instance-describe', \
            'test', 'test-aws', 'test-gcloud', 'test-cloud']

    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        if len(sys.argv) >= 2:
            print('Unimplemented or unrecognized command: ' + ' '.join(sys.argv[1:]))
        print_usage()
        sys.exit(-1)

    cmd = sys.argv[1]

    if cmd == 'init':
        Runtime.run(CmdInit, parse_config=False)
    elif cmd == 'run':
        Runtime.run(CmdRun)
    elif cmd == 'repro':
        Runtime.run(CmdRepro)
    elif cmd == 'sync':
        Runtime.run(CmdDataSync)
    elif cmd == 'import':
        Runtime.run(CmdImportBulk)
    elif cmd == 'import-file':
        Runtime.run(CmdImportFile)
    elif cmd == 'remove':
        Runtime.run(CmdDataRemove)
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
