from __future__ import print_function

"""
main entry point / argument parsing for dvc

NB: for ${REASONS}, cannot be named dvc.py.  trust me.
"""

import sys

from dvc.runtime import Runtime
from dvc.command.init import CmdInit
from dvc.command.remove import CmdDataRemove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataSync
from dvc.command.data_import import CmdDataImport


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
            '')
    print('\n'.join(usage))

if __name__ == '__main__':
    cmds = ['init', 'run', 'sync', 'repro', 'data', 'data-sync', 'data-remove', 'data-import', 'cloud', \
            'cloud', 'cloud-run', 'cloud-instance-create', 'cloud-instance-remove', 'cloud-instance-describe']
    cmds_expand = {'data':  ['sync', 'remove', 'import'],
                   'cloud': ['run', 'instance-create', 'instance-remove', 'instance-describe']
                  }

    if len(sys.argv) < 2 or sys.argv[1] not in cmds:
        print('Unimplemented or unrecognized command. ' + ' '.join(sys.argv[1]))
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
    elif cmd == 'data-import' or (cmd == 'data' and subcmd == 'import'):
        Runtime.run(CmdDataImport, args_start_loc=argv_offset)
    elif cmd == 'data-remove' or (cmd == 'data' and subcmd == 'remove'):
        Runtime.run(CmdDataRemove, args_start_loc=argv_offset)
    elif cmd == 'cloud-run' or (cmd == 'cloud' and subcmd == 'run'):
        print('cloud-run unimplemented')
    elif cmd == 'cloud-instance-create' or (cmd == 'cloud' and subcmd == 'instance-create'):
        print('cloud-instance-create unimplemented')
    elif cmd == 'clould-instance-remove' or (cmd == 'cloud' and subcmd == 'instance-remove'):
        print('cloud-instance-remove unimplemented')
    elif cmd == 'cloud-instance-describe' or (cmd == 'cloud' and subcmd == 'instance-describe'):
        print('cloud-instance-describe unimplemented')
    else:
        print('Unimplemented or unrecognized command. ' + ' '.join(sys.argv[1]))
        print_usage()
        sys.exit(-1)
