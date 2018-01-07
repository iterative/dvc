from dvc.cli import parse_args

def main(argv=None):
    args = parse_args(argv)
    return args.func(args).run_cmd()
