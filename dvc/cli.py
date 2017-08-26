from __future__ import print_function

import argparse
from multiprocessing import cpu_count

from dvc.command.init import CmdInit
from dvc.command.remove import CmdRemove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataSync, CmdDataPush, CmdDataPull, CmdDataStatus
from dvc.command.lock import CmdLock
from dvc.command.gc import CmdGC
from dvc.command.import_file import CmdImportFile
from dvc.command.target import CmdTarget
from dvc.command.instance_create import CmdInstanceCreate
from dvc.command.config import CmdConfig
from dvc.command.visual import CmdVisual
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

    # Parent parser used in sync/pull/push
    parent_sync_parser = argparse.ArgumentParser(
                        add_help=False,
                        parents=[parent_parser])
    parent_sync_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='File or directory to sync.')
    parent_sync_parser.add_argument('-j',
                        '--jobs',
                        type=int,
                        default=cpu_count(),
                        help='Number of jobs to run simultaneously.')

    # Sync
    sync_parser = subparsers.add_parser(
                        'sync',
                        parents=[parent_sync_parser],
                        help='Synchronize data file with cloud (cloud settings already setup.')
    sync_parser.set_defaults(func=CmdDataSync)

    # Pull
    pull_parser = subparsers.add_parser(
                        'pull',
                        parents=[parent_sync_parser],
                        help='Pull data files from the cloud')
    pull_parser.set_defaults(func=CmdDataPull)

    # Push
    push_parser = subparsers.add_parser(
                        'push',
                        parents=[parent_sync_parser],
                        help='Push data files to the cloud')
    push_parser.set_defaults(func=CmdDataPush)

    # Status
    status_parser = subparsers.add_parser(
                        'status',
                        parents=[parent_sync_parser],
                        help='Show status for data files')
    status_parser.set_defaults(func=CmdDataStatus)

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
    import_parser.add_argument('-c',
                        '--continue',
                        dest='cont',
                        action='store_true',
                        default=False,
                        help='Resume downloading file from url')
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

    # Cloud
    ex_parser = subparsers.add_parser(
                        'ex',
                        parents=[parent_parser],
                        help='Experimental commands')
    ex_subparsers = ex_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc cloud CMD --help for command-specific help')

    cloud_parser = ex_subparsers.add_parser(
                        'cloud',
                        parents=[parent_parser],
                        help='Cloud manipulation')
    cloud_subparsers = cloud_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc cloud CMD --help for command-specific help')

    # Instance create
    instance_create_parser = cloud_subparsers.add_parser(
                        'create',
                        help='Create cloud instance')
    instance_create_parser.add_argument('name',
                                        # metavar='',
                                        nargs='?',
                                        help='Instance name.')
    instance_create_parser.add_argument('-c',
                                        '--cloud',
                                        # metavar='',
                                        help='Cloud: AWS, GCP.')
    instance_create_parser.add_argument('-t',
                                        '--type',
                                        # metavar='',
                                        help='Instance type.')
    instance_create_parser.add_argument('-i',
                                        '--image',
                                        # metavar='',
                                        help='Instance image.')
    instance_create_parser.add_argument('--spot-price',
                                        metavar='PRICE',
                                        help='Spot instance price in $ i.e. 1.54')
    instance_create_parser.add_argument('--spot-timeout',
                                        metavar='TIMEOUT',
                                        type=int,
                                        help='Spot instances waiting timeout in seconds.')
    instance_create_parser.add_argument('--keypair-name',
                                        metavar='KEYPAIR',
                                        help='The name of key pair for instance launch')
    instance_create_parser.add_argument('--keypair-dir',
                                        metavar='DIR',
                                        help='The directory of key pairs')
    instance_create_parser.add_argument('--security-group',
                                        metavar='GROUP',
                                        help='Security group')
    instance_create_parser.add_argument('--region',
                                        help='Region')
    instance_create_parser.add_argument('--zone',
                                        help='Zone')
    instance_create_parser.add_argument('--subnet-id',
                                        metavar='SUBNET',
                                        help='Subnet ID')
    instance_create_parser.add_argument('--storage',
                                        # metavar='',
                                        help='The name of attachable storage volume.')
    # WHERE EBS IS?
    instance_create_parser.add_argument('--monitoring',
                                        action='store_true',
                                        default=False,
                                        help='Enable EC2 instance monitoring')
    instance_create_parser.add_argument('--ebs-optimized',
                                        action='store_true',
                                        default=False,
                                        help='Enable EBS I\O optimization')
    instance_create_parser.add_argument('--disks-to-ride0',
                                        action='store_true',
                                        default=False,
                                        help='Detect all ephemeral disks and stripe together in raid-0')
    instance_create_parser.set_defaults(func=CmdInstanceCreate)

    # Config
    config_parser = subparsers.add_parser(
                        'config',
                        parents=[parent_parser],
                        help='Get or set repository options')
    config_parser.add_argument('-u',
                        '--unset',
                        default=False,
                        action='store_true',
                        help='Unset option')
    config_parser.add_argument('name',
                        help='Option name')
    config_parser.add_argument('value',
                        nargs='?',
                        default=None,
                        help='Option value')
    config_parser.set_defaults(func=CmdConfig)

    # Visual
    vz_parser = subparsers.add_parser(
                        'visual',
                        parents=[parent_parser],
                        help='Create a dependency graph for data')
    vz_parser.add_argument(
                        'target',
                        nargs='*',
                        help='Target data')
    vz_parser.set_defaults(func=CmdVisual)

    if isinstance(argv, str):
        argv = argv.split()

    return parser.parse_args(argv)
