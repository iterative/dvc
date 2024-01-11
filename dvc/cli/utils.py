import argparse


class DictAction(argparse.Action):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("metavar", "<name>=<value>")
        super().__init__(*args, **kwargs)

    def __call__(self, parser, args, values, option_string=None):  # noqa: ARG002
        d = getattr(args, self.dest) or {}

        if isinstance(values, list):
            kvs = values
        else:
            kvs = [values]

        for kv in kvs:
            key, value = kv.split("=", 1)
            if not value:
                raise argparse.ArgumentError(
                    self,
                    f'Could not parse argument "{values}" as k1=v1 k2=v2 ... format',
                )
            d[key] = value

        setattr(args, self.dest, d)


def append_doc_link(help_message, path):
    from dvc.utils import format_link

    if not path:
        return help_message
    doc_base = "https://man.dvc.org/"
    return f"{help_message}\nDocumentation: {format_link(doc_base + path)}"


def hide_subparsers_from_help(subparsers):
    # metavar needs to be explicitly set in order to hide subcommands
    # from the 'positional arguments' choices list
    # see: https://bugs.python.org/issue22848
    # Need to set `add_help=False`, but avoid setting `help`
    # (not even to `argparse.SUPPPRESS`).
    # NOTE: The argument is the parent subparser, not the subcommand parser.
    cmds = [cmd for cmd, parser in subparsers.choices.items() if parser.add_help]
    subparsers.metavar = "{{{}}}".format(",".join(cmds))
