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
    cmds = ['init', 'run', 'sync', 'repro', 'data', 'data-sync', 'remove', 'import', 'import-file', 'cloud', \
            'cloud', 'cloud-run', 'cloud-instance-create', 'cloud-instance-remove', 'cloud-instance-describe', \
            'test', 'test-aws', 'test-gcloud', 'test-cloud']
    cmds_expand = {'data':  ['sync'],
                   'cloud': ['run', 'instance-create', 'instance-remove', 'instance-describe'],
                   'test':  ['aws', 'cloud', 'gcloud'],
                  }

    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        if len(sys.argv) >= 2:
            print('Unimplemented or unrecognized command: ' + ' '.join(sys.argv[1:]))
        print_usage()
        sys.exit(-1)

    cmd = sys.argv[1]
    subcmd = None
    if cmd in cmds_expand:
        if (len(sys.argv) < 3 or sys.argv[2] not in cmds_expand[cmd]):
            print('for command %s, eligible actions are %s' % (cmd, cmds_expand[cmd]))
            print_usage()
            sys.exit(-1)
        else:
            subcmd = sys.argv[2]

    argv_offset = 2 + (0 if subcmd == None else 1)
    if cmd == 'init':
        Runtime.run(CmdInit, parse_config=False, args_start_loc=2)
    elif cmd == 'run':
        Runtime.run(CmdRun, args_start_loc=2)
    elif cmd == 'repro':
        Runtime.run(CmdRepro, args_start_loc=2)
    elif cmd == 'data-sync' or (cmd == 'data' and subcmd == 'sync'):
        Runtime.run(CmdDataSync, args_start_loc=argv_offset)
    elif cmd == 'import':
        Runtime.run(CmdImportBulk, args_start_loc=argv_offset)
    elif cmd == 'import-file':
        Runtime.run(CmdImportFile, args_start_loc=argv_offset)
    elif cmd == 'remove':
        Runtime.run(CmdDataRemove, args_start_loc=argv_offset)
    elif cmd == 'cloud-run' or (cmd == 'cloud' and subcmd == 'run'):
        print('cloud-run unimplemented')
    elif cmd == 'cloud-instance-create' or (cmd == 'cloud' and subcmd == 'instance-create'):
        print('cloud-instance-create unimplemented')
    elif cmd == 'clould-instance-remove' or (cmd == 'cloud' and subcmd == 'instance-remove'):
        print('cloud-instance-remove unimplemented')
    elif cmd == 'cloud-instance-describe' or (cmd == 'cloud' and subcmd == 'instance-describe'):
        print('cloud-instance-describe unimplemented')

    elif cmd == 'test-aws' or (cmd == 'test' and subcmd == 'aws'):
        print('TODO: test aws credentials')
    elif cmd == 'test-gcloud' or (cmd=='test' and subcmd in ['cloud', 'gcloud']):
        Runtime.run(CmdTest, args_start_loc=argv_offset)
    else:
        print('Unimplemented or unrecognized command. ' + ' '.join(sys.argv[1]))
        print_usage()
        sys.exit(-1)
