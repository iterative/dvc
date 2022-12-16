def fix_subparsers(subparsers):
    """Workaround for bug in Python 3. See more info at:
    https://bugs.python.org/issue16308
    https://github.com/iterative/dvc/issues/769

    Args:
        subparsers: subparsers to fix.
    """
    subparsers.required = True
    subparsers.dest = "cmd"


def append_doc_link(help_message, path):
    from dvc.utils import format_link

    if not path:
        return help_message
    doc_base = "https://man.dvc.org/"
    return "{message}\nDocumentation: {link}".format(
        message=help_message, link=format_link(doc_base + path)
    )


def hide_subparsers_from_help(subparsers):
    # metavar needs to be explicitly set in order to hide subcommands
    # from the 'positional arguments' choices list
    # see: https://bugs.python.org/issue22848
    # Need to set `add_help=False`, but avoid setting `help`
    # (not even to `argparse.SUPPPRESS`).
    # NOTE: The argument is the parent subparser, not the subcommand parser.
    cmds = [
        cmd for cmd, parser in subparsers.choices.items() if parser.add_help
    ]
    subparsers.metavar = "{{{}}}".format(",".join(cmds))
