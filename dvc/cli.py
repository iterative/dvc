from __future__ import print_function

import os
import sys
import argparse
import configparser
from multiprocessing import cpu_count

from dvc.command.init import CmdInit
from dvc.command.remove import CmdRemove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataSync
from dvc.command.lock import CmdLock
from dvc.command.gc import CmdGC
from dvc.command.import_file import CmdImportFile
from dvc.command.target import CmdTarget
from dvc.command.test import CmdTest
from dvc.config import Config
from dvc import VERSION

def parse_args(argv=None):
    # Common args
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help='Be quiet.')
    parent_parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Be verbose.')
    parent_parser.add_argument('-G',
                        '--no-git-actions',
                        action='store_true',
                        default=False,
                        help='Skip all git actions including reproducibility check and commits.')

    # Main parser
    desc = 'Data Version Control'
    parser = argparse.ArgumentParser(
                        prog='dvc',
                        description=desc,
                        parents=[parent_parser],
                        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('-V',
                        '--version',
                        action='version',
                        version='%(prog)s ' + VERSION)

    # Sub commands
    subparsers = parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc CMD --help for command-specific help')

    # Init
    init_parser = subparsers.add_parser(
                        'init',
                        parents=[parent_parser],
                        help='Initialize dvc over a directory (should already be a git dir).')
    init_parser.add_argument(
                        '--data-dir',
                        default='data',
                        help='Data directory.')
    init_parser.add_argument(
                        '--cache-dir',
                        default='.cache',
                        help='Cache directory.')
    init_parser.add_argument(
                        '--state-dir',
                        default='.state',
                        help='State directory.')
    init_parser.add_argument(
                        '--target-file',
                        default=Config.TARGET_FILE_DEFAULT,
                        help='Target file.')
    init_parser.set_defaults(func=CmdInit)

    # Run
    run_parser = subparsers.add_parser(
                        'run',
                        parents=[parent_parser],
                        help='Run command')
    run_parser.add_argument(
                        '--stdout',
                        help='Output std output to a file.')
    run_parser.add_argument(
                        '--stderr',
                        help='Output std error to a file.')
    run_parser.add_argument('-i',
                        '--input',
                        action='append',
                        help='Declare input data items for reproducible cmd.')
    run_parser.add_argument('-o',
                        '--output',
                        action='append',
                        help='Declare output data items for reproducible cmd.')
    run_parser.add_argument('-c',
                        '--code',
                        action='append',
                        help='Code dependencies which produce the output.')
    run_parser.add_argument(
                        '--shell',
                        action='store_true',
                        default=False,
                        help='Shell command')
    run_parser.add_argument('-l',
                        '--lock',
                        action='store_true',
                        default=False,
                        help='Lock data item - disable reproduction.')
    run_parser.add_argument(
                        'command',
                        help='Command to execute')
    run_parser.add_argument(
                        'args',
                        nargs=argparse.REMAINDER,
                        help='Arguments of a command')
    run_parser.set_defaults(func=CmdRun)

    # Sync
    sync_parser = subparsers.add_parser(
                        'sync',
                        parents=[parent_parser],
                        help='Synchronize data file with cloud (cloud settings already setup.')
    sync_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='File or directory to sync.')
    sync_parser.add_argument('-j',
                        '--jobs',
                        type=int,
                        default=cpu_count(),
                        help='Number of jobs to run simultaneously.')
    sync_parser.set_defaults(func=CmdDataSync)

    # Repro
    repro_parser = subparsers.add_parser(
                        'repro',
                        parents=[parent_parser],
                        help='Reproduce data')
    repro_parser.add_argument(
                        'target',
                        nargs='*',
                        help='Data items to reproduce.')
    repro_parser.add_argument('-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Reproduce even if dependencies were not changed.')
    repro_parser.add_argument('-s',
                        '--single-item',
                        action='store_true',
                        default=False,
                        help='Reproduce only single data item without recursive dependencies check.')
    repro_parser.set_defaults(func=CmdRepro)

    # Remove
    remove_parser = subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        help='Remove data item from data directory.')
    remove_parser.add_argument('target',
                        nargs='*',
                        help='Target to remove - file or directory.')
    remove_parser.add_argument('-l',
                        '--keep-in-cloud',
                        action='store_true',
                        default=False,
                        help='Do not remove data from cloud.')
    remove_parser.add_argument('-r',
                        '--recursive',
                        action='store_true',
                        help='Remove directory recursively.')
    remove_parser.add_argument('-c',
                        '--keep-in-cache',
                        action='store_true',
                        default=False,
                        help='Do not remove data from cache.')
    remove_parser.set_defaults(func=CmdRemove)

    # Import
    import_parser = subparsers.add_parser(
                        'import',
                        parents=[parent_parser],
                        help='Import file to data directory.')
    import_parser.add_argument(
                        'input',
                        nargs='+',
                        help='Input file/files.')
    import_parser.add_argument(
                        'output',
                        help='Output file/directory.')
    import_parser.add_argument('-l',
                        '--lock',
                        action='store_true',
                        default=False,
                        help='Lock data item - disable reproduction.')
    import_parser.add_argument('-j',
                        '--jobs',
                        type=int,
                        default=cpu_count(),
                        help='Number of jobs to run simultaneously.')
    import_parser.set_defaults(func=CmdImportFile)

    # Lock
    lock_parser = subparsers.add_parser(
                        'lock',
                        parents=[parent_parser],
                        help='Lock')
    lock_parser.add_argument('-l',
                        '--lock',
                        action='store_true',
                        default=False,
                        help='Lock data item - disable reproduction.')
    lock_parser.add_argument('-u',
                        '--unlock',
                        action='store_true',
                        default=False,
                        help='Unlock data item - enable reproduction.')
    lock_parser.add_argument(
                        'files',
                        nargs='*',
                        help='Data items to lock or unlock.')
    lock_parser.set_defaults(func=CmdLock)

    # Garbage collector
    gc_parser = subparsers.add_parser(
                        'gc',
                        parents=[parent_parser],
                        help='Collect garbage')
    gc_parser.add_argument('target',
                        nargs='*',
                        help='Target to remove - file or directory.')
    gc_parser.add_argument('-l',
                        '--keep-in-cloud',
                        action='store_true',
                        default=False,
                        help='Do not remove data from cloud.')
    gc_parser.add_argument('-r',
                        '--recursive',
                        action='store_true',
                        help='Remove directory recursively.')
    gc_parser.add_argument('-c',
                        '--keep-in-cache',
                        action='store_false',
                        default=False,
                        help='Do not remove data from cache.')
    gc_parser.set_defaults(func=CmdGC)

    # Target
    target_parser = subparsers.add_parser(
                        'target',
                        parents=[parent_parser],
                        help='Set default target')
    target_parser.add_argument('target_file',
                        nargs='?',
                        help='Target data item.')
    target_parser.add_argument('-u',
                        '--unset',
                        action='store_true',
                        default=False,
                        help='Reset target.')
    target_parser.set_defaults(func=CmdTarget)

    if isinstance(argv, str):
        argv = argv.split()

    return parser.parse_args(argv)
