from dvc.cli import parse_args

def main():
    args = parse_args()
    return args.func(args).run_cmd()
