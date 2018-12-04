from __future__ import print_function

import os
import sys
import argparse

from dvc.command.init import CmdInit
from dvc.command.destroy import CmdDestroy
from dvc.command.remove import CmdRemove
from dvc.command.move import CmdMove
from dvc.command.unprotect import CmdUnprotect
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
from dvc.command.pipeline import CmdPipelineShow, CmdPipelineList
from dvc.command.daemon import CmdDaemonUpdater, CmdDaemonAnalytics
from dvc.exceptions import DvcParserError
from dvc.logger import Logger
from dvc import VERSION


def _fix_subparsers(subparsers):
    # NOTE: Workaround for bug in Python 3
    # More info at:
    #  https://bugs.python.org/issue16308
    #  https://github.com/iterative/dvc/issues/769
    if sys.version_info[0] == 3:  # pragma: no cover
        subparsers.required = True
        subparsers.dest = 'cmd'


class DvcParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('{}{}\n'.format(Logger.error_prefix(), message))
        self.print_help()
        raise DvcParserError()


class VersionAction(argparse.Action):  # pragma: no cover
    def __call__(self, parser, namespace, values, option_string=None):
        print(VERSION)
        sys.exit(0)


def parse_args(argv=None):
    # Common args
    parent_parser = argparse.ArgumentParser(add_help=False)

    log_level_group = parent_parser.add_mutually_exclusive_group()
    log_level_group.add_argument(
                        '-q',
                        '--quiet',
                        action='store_true',
                        default=False,
                        help='Be quiet.')
    log_level_group.add_argument(
                        '-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Be verbose.')

    # Main parser
    desc = 'Data Version Control'
    parser = DvcParser(prog='dvc',
                       description=desc,
                       parents=[parent_parser],
                       formatter_class=argparse.RawTextHelpFormatter)

    # NOTE: On some python versions action='version' prints to stderr
    # instead of stdout https://bugs.python.org/issue18920
    parser.add_argument('-V',
                        '--version',
                        action=VersionAction,
                        nargs=0,
                        help='Show program\'s version.')

    # Sub commands
    subparsers = parser.add_subparsers(
                        title='Available Commands',
                        metavar='COMMAND',
                        dest='cmd',
                        help='Use dvc COMMAND --help for command-specific '
                             'help.')

    _fix_subparsers(subparsers)

    # Init
    INIT_HELP = 'Initialize dvc over a directory ' \
                '(should already be a git dir).'
    init_parser = subparsers.add_parser(
                        'init',
                        parents=[parent_parser],
                        description=INIT_HELP,
                        help=INIT_HELP)
    init_parser.add_argument(
                        '--no-scm',
                        action='store_true',
                        default=False,
                        help="Initiate dvc in directory that is "
                             "not tracked by any scm tool(e.g. git).")
    init_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help="Overwrite '.dvc' if it exists. Will remove "
                             "all local cache.")
    init_parser.set_defaults(func=CmdInit)

    # Destroy
    DESTROY_HELP = "Destroy dvc. Will remove all project's information, " \
                   "data files and cache."
    destroy_parser = subparsers.add_parser(
                        'destroy',
                        parents=[parent_parser],
                        description=DESTROY_HELP,
                        help=DESTROY_HELP)
    destroy_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Force destruction.')
    destroy_parser.set_defaults(func=CmdDestroy)

    # Add
    ADD_HELP = 'Add files/directories to dvc.'
    add_parser = subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        description=ADD_HELP,
                        help=ADD_HELP)
    add_parser.add_argument(
                        '-R',
                        '--recursive',
                        action='store_true',
                        default=False,
                        help='Recursively add each file under the directory.')
    add_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='Input files/directories.')
    add_parser.set_defaults(func=CmdAdd)

    # Import
    IMPORT_HELP = 'Import files from URL.'
    import_parser = subparsers.add_parser(
                        'import',
                        parents=[parent_parser],
                        description=IMPORT_HELP,
                        help=IMPORT_HELP)
    import_parser.add_argument(
                        'url',
                        help="URL. Supported urls: "
                             "'/path/to/file', "
                             "'C:\\\\path\\to\\file', "
                             "'https://example.com/path/to/file', "
                             "'s3://bucket/path/to/file', "
                             "'gs://bucket/path/to/file', "
                             "'hdfs://example.com/path/to/file', "
                             "'ssh://example.com:/path/to/file', "
                             "'remote://myremote/path/to/file'(see "
                             "`dvc remote` commands). ")
    import_parser.add_argument(
                        'out',
                        nargs='?',
                        help='Output.')
    import_parser.set_defaults(func=CmdImport)

    # Checkout
    CHECKOUT_HELP = 'Checkout data files from cache.'
    checkout_parser = subparsers.add_parser(
                        'checkout',
                        parents=[parent_parser],
                        description=CHECKOUT_HELP,
                        help=CHECKOUT_HELP)
    checkout_parser.add_argument(
                        '-d',
                        '--with-deps',
                        action='store_true',
                        default=False,
                        help='Checkout all dependencies of the '
                             'specified target.')
    checkout_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Do not prompt when removing '
                             'working directory files.')
    checkout_parser.add_argument(
                        'targets',
                        nargs='*',
                        help='DVC files.')
    checkout_parser.set_defaults(func=CmdCheckout)

    # Run
    RUN_HELP = 'Generate a stage file from a given ' \
               'command and execute the command.'
    run_parser = subparsers.add_parser(
                        'run',
                        parents=[parent_parser],
                        description=RUN_HELP,
                        help=RUN_HELP)
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
                        help="Specify name of the stage file. It should be "
                             "either 'Dvcfile' or have a '.dvc' suffix (e.g. "
                             "'prepare.dvc', 'clean.dvc', etc) in order for "
                             "dvc to be able to find it later. By default "
                             "the first output basename + .dvc is used as "
                             "a stage filename.")
    run_parser.add_argument(
                        '-c',
                        '--cwd',
                        default=os.path.curdir,
                        help='Directory within your project to run your '
                             'command and place stage file in.')
    run_parser.add_argument(
                        '--no-exec',
                        action='store_true',
                        default=False,
                        help="Only create stage file without actually "
                             "running it.")
    run_parser.add_argument(
                        '-y',
                        '--yes',
                        action='store_true',
                        default=False,
                        help="(OBSOLETED, use --overwrite-dvcfile instead) "
                             "Automatic 'yes' answer to all prompts. E.g. "
                             "when '.dvc' file exists and dvc asks if you "
                             "want to overwrite it.")
    run_parser.add_argument(
                        '--overwrite-dvcfile',
                        action='store_true',
                        default=False,
                        help="Overwrite existing dvc file without asking "
                             "for confirmation.")
    run_parser.add_argument(
                        '--ignore-build-cache',
                        action='store_true',
                        default=False,
                        help="Run this stage even if it has been already "
                        "ran with the same command/dependencies/outputs/etc "
                        "before.")
    run_parser.add_argument(
                        '--remove-outs',
                        action='store_true',
                        default=False,
                        help="Remove outputs before running the command.")
    run_parser.add_argument(
                        'command',
                        nargs=argparse.REMAINDER,
                        help='Command or command file to execute.')
    run_parser.set_defaults(func=CmdRun)

    # Parent parser used in pull/push/status
    parent_cache_parser = argparse.ArgumentParser(
                        add_help=False,
                        parents=[parent_parser])
    parent_cache_parser.add_argument(
                        '-j',
                        '--jobs',
                        type=int,
                        default=None,
                        help='Number of jobs to run simultaneously.')
    parent_cache_parser.add_argument(
                        '--show-checksums',
                        action='store_true',
                        default=False,
                        help='Show checksums instead of file names.')
    parent_cache_parser.add_argument(
                        'targets',
                        nargs='*',
                        default=None,
                        help='DVC files.')

    # Pull
    PULL_HELP = 'Pull data files from the cloud.'
    pull_parser = subparsers.add_parser(
                        'pull',
                        parents=[parent_cache_parser],
                        description=PULL_HELP,
                        help=PULL_HELP)
    pull_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to pull from.')
    pull_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all branches.')
    pull_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all tags.')
    pull_parser.add_argument(
                        '-d',
                        '--with-deps',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all dependencies of the '
                             'specified target.')
    pull_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Do not prompt when removing '
                             'working directory files.')
    pull_parser.set_defaults(func=CmdDataPull)

    # Push
    PUSH_HELP = 'Push data files to the cloud.'
    push_parser = subparsers.add_parser(
                        'push',
                        parents=[parent_cache_parser],
                        description=PUSH_HELP,
                        help=PUSH_HELP)
    push_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to push to.')
    push_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Push cache for all branches.')
    push_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Push cache for all tags.')
    push_parser.add_argument(
                        '-d',
                        '--with-deps',
                        action='store_true',
                        default=False,
                        help='Push cache for all dependencies of the '
                             'specified target.')
    push_parser.set_defaults(func=CmdDataPush)

    # Fetch
    FETCH_HELP = 'Fetch data files from the cloud.'
    fetch_parser = subparsers.add_parser(
                        'fetch',
                        parents=[parent_cache_parser],
                        description=FETCH_HELP,
                        help=FETCH_HELP)
    fetch_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to fetch from.')
    fetch_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all branches.')
    fetch_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all tags.')
    fetch_parser.add_argument(
                        '-d',
                        '--with-deps',
                        action='store_true',
                        default=False,
                        help='Fetch cache for all dependencies of the '
                             'specified target.')
    fetch_parser.set_defaults(func=CmdDataFetch)

    # Status
    STATUS_HELP = 'Show the project status.'
    status_parser = subparsers.add_parser(
                        'status',
                        parents=[parent_cache_parser],
                        description=STATUS_HELP,
                        help=STATUS_HELP)
    status_parser.add_argument(
                        '-c',
                        '--cloud',
                        action='store_true',
                        default=False,
                        help='Show status of a local cache compared to a '
                             'remote repository.')
    status_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to compare local cache to.')
    status_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Show status of a local cache compared to a '
                             'remote repository for all branches.')
    status_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Show status of a local cache compared to a '
                             'remote repository for all tags.')
    status_parser.add_argument(
                        '-d',
                        '--with-deps',
                        action='store_true',
                        default=False,
                        help='Show status of a local cache compared to a '
                             'remote repository for all dependencies of the '
                             'specified target.')
    status_parser.set_defaults(func=CmdDataStatus)

    # Repro
    REPRO_HELP = 'Reproduce DVC file. Default file name - \'Dvcfile\'.'
    repro_parser = subparsers.add_parser(
                        'repro',
                        parents=[parent_parser],
                        description=REPRO_HELP,
                        help=REPRO_HELP)
    repro_parser.add_argument(
                        'targets',
                        nargs='*',
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
                        help='Directory within your project to '
                             'reroduce from.')
    repro_parser.add_argument(
                        '-m',
                        '--metrics',
                        action='store_true',
                        default=False,
                        help='Show metrics after reproduction.')
    repro_parser.add_argument(
                        '--dry',
                        action='store_true',
                        default=False,
                        help='Only print the commands that would be executed '
                             'without actually executing.')
    repro_parser.add_argument(
                        '-i',
                        '--interactive',
                        action='store_true',
                        default=False,
                        help='Ask for confirmation before reproducing each '
                             'stage.')
    repro_parser.add_argument(
                        '-p',
                        '--pipeline',
                        action='store_true',
                        default=False,
                        help='Reproduce the whole pipeline that the '
                             'specified stage file belongs to.')
    repro_parser.add_argument(
                        '-P',
                        '--all-pipelines',
                        action='store_true',
                        default=False,
                        help='Reproduce all pipelines in the project.')
    repro_parser.set_defaults(func=CmdRepro)

    # Remove
    REMOVE_HELP = 'Remove outputs of DVC file.'
    remove_parser = subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        description=REMOVE_HELP,
                        help=REMOVE_HELP)
    remove_parser_group = remove_parser.add_mutually_exclusive_group()
    remove_parser_group.add_argument(
                        '-o',
                        '--outs',
                        action='store_true',
                        default=True,
                        help='Only remove DVC file outputs.(default)')
    remove_parser_group.add_argument(
                        '-p',
                        '--purge',
                        action='store_true',
                        default=False,
                        help='Remove DVC file and all its outputs.')
    remove_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Force purge.')
    remove_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    remove_parser.set_defaults(func=CmdRemove)

    # Move
    MOVE_HELP = 'Move output of DVC file.'
    move_parser = subparsers.add_parser(
                        'move',
                        parents=[parent_parser],
                        description=MOVE_HELP,
                        help=MOVE_HELP)
    move_parser.add_argument(
                        'src',
                        help='Source.')
    move_parser.add_argument(
                        'dst',
                        help='Destination.')
    move_parser.set_defaults(func=CmdMove)

    # Unprotect
    UNPROTECT_HELP = 'Unprotect data file/directory.'
    unprotect_parser = subparsers.add_parser(
                        'unprotect',
                        parents=[parent_parser],
                        description=UNPROTECT_HELP,
                        help=UNPROTECT_HELP)
    unprotect_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='Data files/directory.')
    unprotect_parser.set_defaults(func=CmdUnprotect)

    # Garbage collector
    GC_HELP = 'Collect garbage.'
    gc_parser = subparsers.add_parser(
                        'gc',
                        parents=[parent_parser],
                        description=GC_HELP,
                        help=GC_HELP)
    gc_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Collect garbage for all branches.')
    gc_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Collect garbage for all tags.')
    gc_parser.add_argument(
                        '-c',
                        '--cloud',
                        action='store_true',
                        default=False,
                        help='Collect garbage in remote repository.')
    gc_parser.add_argument(
                        '-r',
                        '--remote',
                        help='Remote repository to collect garbage in.')
    gc_parser.add_argument(
                        '-f',
                        '--force',
                        action='store_true',
                        default=False,
                        help='Force garbage collection.')
    gc_parser.add_argument(
                        '-j',
                        '--jobs',
                        type=int,
                        default=None,
                        help='Number of jobs to run simultaneously.')
    gc_parser.add_argument(
                        '-p',
                        '--projects',
                        type=str,
                        nargs='*',
                        default=None,
                        help='Collect garbage for all given projects.')
    gc_parser.set_defaults(func=CmdGC)

    # Config
    CONFIG_HELP = 'Get or set config options.'
    config_parser = subparsers.add_parser(
                        'config',
                        parents=[parent_parser],
                        description=CONFIG_HELP,
                        help=CONFIG_HELP)
    config_parser.add_argument(
                        '-u',
                        '--unset',
                        default=False,
                        action='store_true',
                        help='Unset option.')
    config_parser.add_argument(
                        'name',
                        help='Option name.')
    config_parser.add_argument(
                        'value',
                        nargs='?',
                        default=None,
                        help='Option value.')
    config_parser.add_argument(
                        '--global',
                        dest='glob',
                        action='store_true',
                        default=False,
                        help='Use global config.')
    config_parser.add_argument(
                        '--system',
                        action='store_true',
                        default=False,
                        help='Use system config.')
    config_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config.')
    config_parser.set_defaults(func=CmdConfig)

    # Remote
    REMOTE_HELP = 'Manage set of tracked repositories.'
    remote_parser = subparsers.add_parser(
                        'remote',
                        parents=[parent_parser],
                        description=REMOTE_HELP,
                        help=REMOTE_HELP)

    remote_subparsers = remote_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc remote CMD --help for '
                             'command-specific help.')

    _fix_subparsers(remote_subparsers)

    REMOTE_ADD_HELP = 'Add remote.'
    remote_add_parser = remote_subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        description=REMOTE_ADD_HELP,
                        help=REMOTE_ADD_HELP)
    remote_add_parser.add_argument(
                        'name',
                        help='Name.')
    remote_add_parser.add_argument(
                        'url',
                        help='URL.')
    remote_add_parser.add_argument(
                        '--global',
                        dest='glob',
                        action='store_true',
                        default=False,
                        help='Use global config.')
    remote_add_parser.add_argument(
                        '--system',
                        action='store_true',
                        default=False,
                        help='Use system config.')
    remote_add_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config.')
    remote_add_parser.add_argument(
                        '-d',
                        '--default',
                        action='store_true',
                        default=False,
                        help='Set as default remote.')
    remote_add_parser.set_defaults(func=CmdRemoteAdd)

    REMOTE_REMOVE_HELP = 'Remove remote.'
    remote_remove_parser = remote_subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        description=REMOTE_REMOVE_HELP,
                        help=REMOTE_REMOVE_HELP)
    remote_remove_parser.add_argument(
                        'name',
                        help='Name')
    remote_remove_parser.add_argument(
                        '--global',
                        dest='glob',
                        action='store_true',
                        default=False,
                        help='Use global config.')
    remote_remove_parser.add_argument(
                        '--system',
                        action='store_true',
                        default=False,
                        help='Use system config.')
    remote_remove_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config.')
    remote_remove_parser.set_defaults(func=CmdRemoteRemove)

    REMOTE_MODIFY_HELP = 'Modify remote.'
    remote_modify_parser = remote_subparsers.add_parser(
                        'modify',
                        parents=[parent_parser],
                        description=REMOTE_MODIFY_HELP,
                        help=REMOTE_MODIFY_HELP)
    remote_modify_parser.add_argument(
                        'name',
                        help='Name.')
    remote_modify_parser.add_argument(
                        'option',
                        help='Option.')
    remote_modify_parser.add_argument(
                        'value',
                        nargs='?',
                        help='Value.')
    remote_modify_parser.add_argument(
                        '-u',
                        '--unset',
                        default=False,
                        action='store_true',
                        help='Unset option.')
    remote_modify_parser.add_argument(
                        '--global',
                        dest='glob',
                        action='store_true',
                        default=False,
                        help='Use global config.')
    remote_modify_parser.add_argument(
                        '--system',
                        action='store_true',
                        default=False,
                        help='Use system config.')
    remote_modify_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config.')
    remote_modify_parser.set_defaults(func=CmdRemoteModify)

    REMOTE_LIST_HELP = 'List remotes.'
    remote_list_parser = remote_subparsers.add_parser(
                        'list',
                        parents=[parent_parser],
                        description=REMOTE_LIST_HELP,
                        help=REMOTE_LIST_HELP)
    remote_list_parser.add_argument(
                        '--global',
                        dest='glob',
                        action='store_true',
                        default=False,
                        help='Use global config.')
    remote_list_parser.add_argument(
                        '--system',
                        action='store_true',
                        default=False,
                        help='Use system config.')
    remote_list_parser.add_argument(
                        '--local',
                        action='store_true',
                        default=False,
                        help='Use local config.')
    remote_list_parser.set_defaults(func=CmdRemoteList)

    # Metrics
    METRICS_HELP = 'Get metrics from all branches.'
    metrics_parser = subparsers.add_parser(
                        'metrics',
                        parents=[parent_parser],
                        description=METRICS_HELP,
                        help=METRICS_HELP)

    metrics_subparsers = metrics_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc metrics CMD --help for '
                             'command-specific help.')

    _fix_subparsers(metrics_subparsers)

    METRICS_SHOW_HELP = 'Show metrics.'
    metrics_show_parser = metrics_subparsers.add_parser(
                        'show',
                        parents=[parent_parser],
                        description=METRICS_SHOW_HELP,
                        help=METRICS_SHOW_HELP)
    metrics_show_parser.add_argument(
                        'path',
                        nargs='?',
                        help='Path to metrics file.')
    metrics_show_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV).')
    metrics_show_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path.')
    metrics_show_group = metrics_show_parser.add_mutually_exclusive_group()
    metrics_show_group.add_argument(
                        '--json-path',
                        help='JSON path.')
    metrics_show_group.add_argument(
                        '--tsv-path',
                        help='TSV path \'row,column\'(e.g. \'1,2\').')
    metrics_show_group.add_argument(
                        '--htsv-path',
                        help='Headed TSV path \'row,column\''
                             '(e.g. \'Name,3\').')
    metrics_show_group.add_argument(
                        '--csv-path',
                        help='CSV path \'row,column\'(e.g. \'1,2\').')
    metrics_show_group.add_argument(
                        '--hcsv-path',
                        help='Headed CSV path \'row,column\''
                             '(e.g. \'Name,3\').')
    metrics_show_parser.add_argument(
                        '-a',
                        '--all-branches',
                        action='store_true',
                        default=False,
                        help='Show metrics for all branches.')
    metrics_show_parser.add_argument(
                        '-T',
                        '--all-tags',
                        action='store_true',
                        default=False,
                        help='Show metrics for all tags.')
    metrics_show_parser.set_defaults(func=CmdMetricsShow)

    METRICS_ADD_HELP = 'Add metrics.'
    metrics_add_parser = metrics_subparsers.add_parser(
                        'add',
                        parents=[parent_parser],
                        description=METRICS_ADD_HELP,
                        help=METRICS_ADD_HELP)
    metrics_add_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV).')
    metrics_add_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path.')
    metrics_add_parser.add_argument(
                        'path',
                        help='Path to metrics file.')
    metrics_add_parser.set_defaults(func=CmdMetricsAdd)

    METRICS_MODIFY_HELP = 'Modify metrics.'
    metrics_modify_parser = metrics_subparsers.add_parser(
                        'modify',
                        parents=[parent_parser],
                        description=METRICS_MODIFY_HELP,
                        help=METRICS_MODIFY_HELP)
    metrics_modify_parser.add_argument(
                        '-t',
                        '--type',
                        help='Type of metrics(RAW/JSON/TSV/HTSV/CSV/HCSV).')
    metrics_modify_parser.add_argument(
                        '-x',
                        '--xpath',
                        help='JSON/TSV/HTSV/CSV/HCSV path.')
    metrics_modify_parser.add_argument(
                        'path',
                        help='Metrics file.')
    metrics_modify_parser.set_defaults(func=CmdMetricsModify)

    METRICS_REMOVE_HELP = 'Remove metrics.'
    metrics_remove_parser = metrics_subparsers.add_parser(
                        'remove',
                        parents=[parent_parser],
                        description=METRICS_REMOVE_HELP,
                        help=METRICS_REMOVE_HELP)
    metrics_remove_parser.add_argument(
                        'path',
                        help='Path to metrics file.')
    metrics_remove_parser.set_defaults(func=CmdMetricsRemove)

    # Install
    INSTALL_HELP = 'Install dvc hooks into the repository.'
    install_parser = subparsers.add_parser(
                        'install',
                        parents=[parent_parser],
                        description=INSTALL_HELP,
                        help=INSTALL_HELP)
    install_parser.set_defaults(func=CmdInstall)

    # Root
    ROOT_HELP = 'Relative path to project\'s directory.'
    root_parser = subparsers.add_parser(
                        'root',
                        parents=[parent_parser],
                        description=ROOT_HELP,
                        help=ROOT_HELP)
    root_parser.set_defaults(func=CmdRoot)

    # Lock
    LOCK_HELP = 'Lock DVC file.'
    lock_parser = subparsers.add_parser(
                        'lock',
                        parents=[parent_parser],
                        description=LOCK_HELP,
                        help=LOCK_HELP)
    lock_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    lock_parser.set_defaults(func=CmdLock)

    # Unlock
    UNLOCK_HELP = 'Unlock DVC file.'
    unlock_parser = subparsers.add_parser(
                        'unlock',
                        parents=[parent_parser],
                        description=UNLOCK_HELP,
                        help=UNLOCK_HELP)
    unlock_parser.add_argument(
                        'targets',
                        nargs='+',
                        help='DVC files.')
    unlock_parser.set_defaults(func=CmdUnlock)

    # Pipeline
    PIPELINE_HELP = 'Manage pipeline.'
    pipeline_parser = subparsers.add_parser(
                        'pipeline',
                        parents=[parent_parser],
                        description=PIPELINE_HELP,
                        help=PIPELINE_HELP)

    pipeline_subparsers = pipeline_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc pipeline CMD --help '
                             'for command-specific help.')

    _fix_subparsers(pipeline_subparsers)

    PIPELINE_SHOW_HELP = 'Show pipeline.'
    pipeline_show_parser = pipeline_subparsers.add_parser(
                        'show',
                        parents=[parent_parser],
                        description=PIPELINE_SHOW_HELP,
                        help=PIPELINE_SHOW_HELP)
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
                        help='Print output files instead of '
                             'paths to DVC files.')
    pipeline_show_parser.add_argument(
                        '--ascii',
                        action='store_true',
                        default=False,
                        help='Output DAG as ASCII.')
    pipeline_show_parser.add_argument(
                        '--dot',
                        help='Write DAG in .dot format.'
    )
    pipeline_show_parser.add_argument(
                        'targets',
                        nargs='*',
                        help="DVC files. 'Dvcfile' by default.")
    pipeline_show_parser.set_defaults(func=CmdPipelineShow)

    PIPELINE_LIST_HELP = 'List pipelines.'
    pipeline_list_parser = pipeline_subparsers.add_parser(
                        'list',
                        parents=[parent_parser],
                        description=PIPELINE_LIST_HELP,
                        help=PIPELINE_LIST_HELP)
    pipeline_list_parser.set_defaults(func=CmdPipelineList)

    # Daemon
    DAEMON_HELP = 'Service daemon.'
    daemon_parser = subparsers.add_parser(
                        'daemon',
                        parents=[parent_parser],
                        description=DAEMON_HELP,
                        help=DAEMON_HELP)

    daemon_subparsers = daemon_parser.add_subparsers(
                        dest='cmd',
                        help='Use dvc daemon CMD --help '
                             'for command-specific help.')

    _fix_subparsers(daemon_subparsers)

    DAEMON_UPDATER_HELP = 'Fetch latest available version.'
    daemon_updater_parser = daemon_subparsers.add_parser(
                        'updater',
                        parents=[parent_parser],
                        description=DAEMON_UPDATER_HELP,
                        help=DAEMON_UPDATER_HELP)
    daemon_updater_parser.set_defaults(func=CmdDaemonUpdater)

    DAEMON_ANALYTICS_HELP = 'Send dvc usage analytics.'
    daemon_analytics_parser = daemon_subparsers.add_parser(
                        'analytics',
                        parents=[parent_parser],
                        description=DAEMON_ANALYTICS_HELP,
                        help=DAEMON_ANALYTICS_HELP)
    daemon_analytics_parser.add_argument(
                        'target',
                        help="Analytics file.")
    daemon_analytics_parser.set_defaults(func=CmdDaemonAnalytics)

    args = parser.parse_args(argv)

    if (issubclass(args.func, CmdRepro)
        or issubclass(args.func, CmdPipelineShow)) \
       and hasattr(args, 'targets') \
       and len(args.targets) == 0 \
       and not (hasattr(args, 'all_pipelines')
       and args.all_pipelines):  # pragma: no cover
        if hasattr(args, 'cwd'):
            cwd = args.cwd
        else:
            cwd = os.curdir
        path = os.path.join(cwd, 'Dvcfile')
        if not os.path.exists(path):
            msg = "default target '{}' does not exist.".format(path)
            if issubclass(args.func, CmdRepro):
                repro_parser.error(msg)
            elif issubclass(args.func, CmdPipelineShow):
                pipeline_show_parser.error(msg)
        args.targets = ['Dvcfile']

    if issubclass(args.func, CmdRun) \
       and len(args.deps) == 0 \
       and len(args.outs) == 0 \
       and len(args.outs_no_cache) == 0 \
       and len(args.command) == 0:  # pragma: no cover
        run_parser.error("Too few arguments. Specify at least one: "
                         "'-d', '-o', '-O', 'command'.")

    return args
