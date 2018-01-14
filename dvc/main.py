from dvc.logger import Logger
from dvc.cli import parse_args

def main(argv=None):
    Logger.init()

    args = parse_args(argv)

    try:
        cmd = args.func(args)
    except Exception as ex:
        Logger.error('Initialization error: {}'.format(str(ex)))
        return 255

    try:
        ret = cmd.run_cmd()
    except Exception as ex:
        Logger.error('Unexpected error: {}'.format(str(ex)))
        return 254

    return ret
