from __future__ import print_function

import os
import sys
import argparse
from multiprocessing import cpu_count

from dvc.command.init import CmdInit
from dvc.command.destroy import CmdDestroy
from dvc.command.remove import CmdRemove
from dvc.command.move import CmdMove
from dvc.command.run import CmdRun
from dvc.command.repro import CmdRepro
from dvc.command.data_sync import CmdDataPush, CmdDataPull, CmdDataFetch
from dvc.command.status import CmdDataStatus
from dvc.command.gc import CmdGC
from dvc.command.add import CmdAdd
from dvc.command.imp import CmdImport
from dvc.command.config import CmdConfig
from dvc.command.checkout import CmdCheckout
from dvc.command.remote import CmdRemoteAdd, CmdRemoteRemove
from dvc.command.remote import CmdRemoteModify, CmdRemoteList
from dvc.command.metrics import CmdMetricsShow, CmdMetricsAdd
from dvc.command.metrics import CmdMetricsRemove, CmdMetricsModify
from dvc.command.install import CmdInstall
from dvc.command.root import CmdRoot
from dvc.command.lock import CmdLock, CmdUnlock
from dvc.command.pipeline import CmdPipelineShow
from dvc import VERSION


def _fix_subparsers(subparsers):
    # NOTE: Workaround for bug in Python 3
    # More info at:
    #  https://bugs.python.org/issue16308
    #  https://github.com/iterative/dvc/issues/769
    if sys.version_info[0] == 3:
        subparsers.required = True
        subparsers.dest = 'cmd'


def parse_args(argv=None):
    # Common args
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
                        '-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help='Be quiet.')
    parent_parser.add_argument(
                        '-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Be verbose.')

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
                        version='%(prog)s ' + VERSION,
                        help='Show program\'s version')

    # Sub commands
    subparsers = parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc CMD --help for command-specific help')

    _fix_subparsers(subparsers)

    # Init
    init_parser = subparsers.add_parser(
                        'init',
                        parents=[parent_parser],
                        help='Initialize dvc over a directory'
                             '(should already be a git dir)')
    init_parser.add_argument(
                        '--no-scm',
                        action='store_true',
                        default=False,
                        help="Initiate dvc in directory that is "
                             "not tracked by any scm tool(e.g. git)")
    init_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Force initialization even if \'.dvc\' exists')
    init_parser.set_defaults(func=CmdInit)

    # Destroy
    destroy_parser = subparsers.add_parser(
                        'destroy',
                        parents=[parent_parser],
                        help='Destroy dvc')
    destroy_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Force destruction')
    destroy_parser.set_defaults(func=CmdDestroy)

    # Add
    add_parser = subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        help='Add files/directories to dvc')
    add_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='Input files/directories')
    add_parser.set_defaults(func=CmdAdd)

    # Import
    import_parser = subparsers.add_parser(
                        'import',
                        parents=[parent_parser],
                        help='Import files from URL')
    import_parser.add_argument(
                        'url',
                        help='URL')
    import_parser.add_argument(
                        'out',
                        help='Output')
    import_parser.set_defaults(func=CmdImport)

    # Checkout
    checkout_parser = subparsers.add_parser(
                        'checkout',
                        parents=[parent_parser],
                        help='Checkout data files from cache')
    checkout_parser.add_argument(
                        'targets',
                        nargs='*',
                        help='DVC files')
    checkout_parser.set_defaults(func=CmdCheckout)

    # Run
    run_parser = subparsers.add_parser(
                        'run',
                        parents=[parent_parser],
                        help='Generate a stage file from a given '
                             'command and execute the command')
    run_parser.add_argument(
                        '-d',
                        '--deps',
                        action='append',
                        default=[],
                        help='Declare dependencies for reproducible cmd.')
    run_parser.add_argument(
                        '-o',
                        '--outs',
                        action='append',
                        default=[],
                        help='Declare output data file or data directory.')
    run_parser.add_argument(
                        '-O',
                        '--outs-no-cache',
                        action='append',
                        default=[],
                        help='Declare output regular file or '
                             'directory (sync to Git, not DVC cache).')
    run_parser.add_argument(
                        '-M',
                        '--metrics-no-cache',
                        action='append',
                        default=[],
                        help='Declare output metric file or '
                             'directory (not cached by DVC).')
    run_parser.add_argument(
                        '-f',
                        '--file',
                        help='Specify name of the state file')
    run_parser.add_argument(
                        '-c',
                        '--cwd',
                        default=os.path.curdir,
                        help='Directory to run your command and place state '
                             'file in')
    run_parser.add_argument(
                        '--no-exec',
                        action='store_true',
                        default=False,
                        help="Only create stage file without actually "
                             "running it")
    run_parser.add_argument(
                        'command',
                        nargs=argparse.REMAINDER,
                        help='Command or command file to execute')
    run_parser.set_defaults(func=CmdRun)

    # Parent parser used in pull/push/status
    parent_cache_parser = argparse.ArgumentParser(
                        add_help=False,
                        parents=[parent_parser])
    parent_cache_parser.add_argument(
                        '-j',
                        '--jobs',
                        type=int,
                        default=8*cpu_count(),
                        help='Number of jobs to run simultaneously.')
    parent_cache_parser.add_argument(
                        'targets',
                        nargs='*',
                        default=None,
                        help='DVC files.')

    # Pull
    pull_parser = subparsers.add_parser(
                        'pull',
                        parents=[parent_cache_parser],
                        help='Pull data files from the cloud')
    pull_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to pull from')
    pull_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all branches.')
    pull_parser.set_defaults(func=CmdDataPull)

    # Push
    push_parser = subparsers.add_parser(
                        'push',
                        parents=[parent_cache_parser],
                        help='Push data files to the cloud')
    push_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to push to')
    push_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Push cache for all branches.')
    push_parser.set_defaults(func=CmdDataPush)

    # Fetch
    fetch_parser = subparsers.add_parser(
                        'fetch',
                        parents=[parent_cache_parser],
                        help='Fetch data files from the cloud')
    fetch_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to fetch from')
    fetch_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all branches.')
    fetch_parser.set_defaults(func=CmdDataFetch)

    # Status
    status_parser = subparsers.add_parser(
                        'status',
                        parents=[parent_cache_parser],
                        help='Show the project status')
    status_parser.add_argument(
                        '-c',
                        '--cloud',
                        action='store_true',
                        default=False,
                        help='Show status of a local cache compared to a '
                             'remote repository')
    status_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to compare local cache to')
    status_parser.set_defaults(func=CmdDataStatus)

    # Repro
    repro_parser = subparsers.add_parser(
                        'repro',
                        parents=[parent_parser],
                        help='Reproduce DVC file. Default file name '
                             '- \'Dvcfile\'')
    repro_parser.add_argument(
                        'targets',
                        nargs='*',
                        default=['Dvcfile'],
                        help='DVC file to reproduce.')
    repro_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Reproduce even if dependencies were not '
                             'changed.')
    repro_parser.add_argument(
                        '-s',
                        '--single-item',
                        action='store_true',
                        default=False,
                        help='Reproduce only single data item without '
                             'recursive dependencies check.')
    repro_parser.add_argument(
                        '-c',
                        '--cwd',
                        default=os.path.curdir,
                        help='Directory to reproduce from.')
    repro_parser.add_argument(
                        '-m',
                        '--metrics',
                        action='store_true',
                        default=False,
                        help='Show metrics after reproduction')
    repro_parser.set_defaults(func=CmdRepro)

    # Remove
    remove_parser = subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        help='Remove outputs of DVC file.')
    remove_parser_group = remove_parser.add_mutually_exclusive_group()
    remove_parser_group.add_argument(
                        '-o',
                        '--outs',
                        action='store_true',
                        default=True,
                        help='Only remove DVC file outputs.')
    remove_parser_group.add_argument(
                        '-p',
                        '--purge',
                        action='store_true',
                        default=False,
                        help='Remove DVC file and all its outputs')
    remove_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    remove_parser.set_defaults(func=CmdRemove)

    # Move
    move_parser = subparsers.add_parser(
                        'move',
                        parents=[parent_parser],
                        help='Move output of DVC file.')
    move_parser.add_argument(
                        'src',
                        help='Source')
    move_parser.add_argument(
                        'dst',
                        help='Destination')
    move_parser.set_defaults(func=CmdMove)

    # Garbage collector
    gc_parser = subparsers.add_parser(
                        'gc',
                        parents=[parent_parser],
                        help='Collect garbage')
    gc_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Collect garbage for all branches.')
    gc_parser.add_argument(
                        '-c',
                        '--cloud',
                        action='store_true',
                        default=False,
                        help='Collect garbage in remote repository')
    gc_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to collect garbage in')
    gc_parser.set_defaults(func=CmdGC)

    # Config
    config_parser = subparsers.add_parser(
                        'config',
                        parents=[parent_parser],
                        help='Get or set config options')
    config_parser.add_argument(
                        '-u',
                        '--unset',
                        default=False,
                        action='store_true',
                        help='Unset option')
    config_parser.add_argument(
                        'name',
                        help='Option name')
    config_parser.add_argument(
                        'value',
                        nargs='?',
                        default=None,
                        help='Option value')
    config_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config')
    config_parser.set_defaults(func=CmdConfig)

    # Remote
    remote_parser = subparsers.add_parser(
                        'remote',
                        parents=[parent_parser],
                        help='Manage set of tracked repositories')

    remote_subparsers = remote_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc remote CMD --help for '
                             'command-specific help')

    _fix_subparsers(remote_subparsers)

    remote_add_parser = remote_subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        help='Add remote')
    remote_add_parser.add_argument(
                        'name',
                        help='Name')
    remote_add_parser.add_argument(
                        'url',
                        help='Url')
    remote_add_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config')
    remote_add_parser.add_argument(
                        '-d',
                        '--default',
                        action='store_true',
                        default=False,
                        help='Set as default remote')
    remote_add_parser.set_defaults(func=CmdRemoteAdd)

    remote_remove_parser = remote_subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        help='Remove remote')
    remote_remove_parser.add_argument(
                        'name',
                        help='Name')
    remote_remove_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config')
    remote_remove_parser.set_defaults(func=CmdRemoteRemove)

    remote_modify_parser = remote_subparsers.add_parser(
                        'modify',
                        parents=[parent_parser],
                        help='Modify remote')
    remote_modify_parser.add_argument(
                        'name',
                        help='Name')
    remote_modify_parser.add_argument(
                        'option',
                        help='Option')
    remote_modify_parser.add_argument(
                        'value',
                        nargs='?',
                        help='Value')
    remote_modify_parser.add_argument(
                        '-u',
                        '--unset',
                        default=False,
                        action='store_true',
                        help='Unset option')
    remote_modify_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config')
    remote_modify_parser.set_defaults(func=CmdRemoteModify)

    remote_list_parser = remote_subparsers.add_parser(
                        'list',
                        parents=[parent_parser],
                        help='List remotes')
    remote_list_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config')
    remote_list_parser.set_defaults(func=CmdRemoteList)

    # Metrics
    metrics_parser = subparsers.add_parser(
                        'metrics',
                        parents=[parent_parser],
                        help='Get metrics from all branches')

    metrics_subparsers = metrics_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc metrics CMD --help for '
                             'command-specific help')

    _fix_subparsers(metrics_subparsers)

    metrics_show_parser = metrics_subparsers.add_parser(
                        'show',
                        parents=[parent_parser],
                        help='Show metrics')
    metrics_show_parser.add_argument(
                        'path',
                        nargs='?',
                        help='Path to metrics file')
    metrics_show_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)')
    metrics_show_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path')
    metrics_show_group = metrics_show_parser.add_mutually_exclusive_group()
    metrics_show_group.add_argument(
                        '--json-path',
                        help='JSON path')
    metrics_show_group.add_argument(
                        '--tsv-path',
                        help='TSV path \'row,column\'(e.g. \'1,2\')')
    metrics_show_group.add_argument(
                        '--htsv-path',
                        help='Headed TSV path \'row,column\'(e.g. \'Name,3\'')
    metrics_show_group.add_argument(
                        '--csv-path',
                        help='CSV path \'row,column\'(e.g. \'1,2\')')
    metrics_show_group.add_argument(
                        '--hcsv-path',
                        help='Headed CSV path \'row,column\'(e.g. \'Name,3\'')
    metrics_show_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Show metrics for all branches')
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    metrics_add_parser = metrics_subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        help='Add metrics')
    metrics_add_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)')
    metrics_add_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path')
    metrics_add_parser.add_argument(
                        'path',
                        help='Path to metrics file')
    metrics_add_parser.set_defaults(func=CmdMetricsAdd)

    metrics_modify_parser = metrics_subparsers.add_parser(
                        'modify',
                        parents=[parent_parser],
                        help='Modify metrics')
    metrics_modify_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV)')
    metrics_modify_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path')
    metrics_modify_parser.add_argument(
                        'path',
                        help='Metrics file')
    metrics_modify_parser.set_defaults(func=CmdMetricsModify)

    metrics_remove_parser = metrics_subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        help='Remove metrics')
    metrics_remove_parser.add_argument(
                        'path',
                        help='Path to metrics file')
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)

    # Install
    install_parser = subparsers.add_parser(
                        'install',
                        parents=[parent_parser],
                        help='Install dvc hooks into the repository')
    install_parser.set_defaults(func=CmdInstall)

    # Root
    root_parser = subparsers.add_parser(
                        'root',
                        parents=[parent_parser],
                        help='Relative path to project\'s directory')
    root_parser.set_defaults(func=CmdRoot)

    # Lock
    lock_parser = subparsers.add_parser(
                        'lock',
                        parents=[parent_parser],
                        help='Lock DVC file')
    lock_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    lock_parser.set_defaults(func=CmdLock)

    # Unlock
    unlock_parser = subparsers.add_parser(
                        'unlock',
                        parents=[parent_parser],
                        help='Unlock DVC file')
    unlock_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    unlock_parser.set_defaults(func=CmdUnlock)

    # Pipeline
    pipeline_parser = subparsers.add_parser(
                        'pipeline',
                        parents=[parent_parser],
                        help='Manage pipeline')

    pipeline_subparsers = pipeline_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc pipeline CMD --help '
                             'for command-specific help')

    _fix_subparsers(pipeline_subparsers)

    pipeline_show_parser = pipeline_subparsers.add_parser(
                        'show',
                        parents=[parent_parser],
                        help='Show pipeline')
    pipeline_show_group = pipeline_show_parser.add_mutually_exclusive_group()
    pipeline_show_group.add_argument(
                        '-c',
                        '--commands',
                        action='store_true',
                        default=False,
                        help='Print commands instead of paths to DVC files.')
    pipeline_show_group.add_argument(
                        '-o',
                        '--outs',
                        action='store_true',
                        default=False,
                        help='Print output files instead of'
                             'paths to DVC files.')
    pipeline_show_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    pipeline_show_parser.set_defaults(func=CmdPipelineShow)

    args = parser.parse_args(argv)

    return args
