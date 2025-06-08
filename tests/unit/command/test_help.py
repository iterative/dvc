import logging
import re
from argparse import SUPPRESS, ArgumentParser
from itertools import takewhile

import pytest
import shtab

from dvc.cli import main
from dvc.cli.parser import get_main_parser


def command_tuples():
    root: tuple[str, ...] = ()
    commands = [root]

    def recurse_parser(parser: ArgumentParser, parents: tuple[str, ...] = root) -> None:
        for positional in parser._get_positional_actions():
            if positional.help != SUPPRESS and isinstance(positional.choices, dict):
                public_cmds = shtab.get_public_subcommands(positional)
                for subcmd, subparser in positional.choices.items():
                    cmd = (*parents, subcmd)
                    if subcmd in public_cmds:
                        commands.append(cmd)
                    recurse_parser(subparser, cmd)

    main_parser = get_main_parser()
    recurse_parser(main_parser)

    # the no. of commands will usually go up,
    # but if we ever remove commands and drop below, adjust the magic number accordingly
    assert len(commands) >= 116
    return sorted(commands)


def ids(values):
    return "-".join(values) or "dvc"


@pytest.mark.parametrize("command_tuples", command_tuples(), ids=ids)
def test_help(caplog, capsys, command_tuples):
    with caplog.at_level(logging.INFO), pytest.raises(SystemExit) as e:
        main([*command_tuples, "--help"])
    assert e.value.code == 0
    assert not caplog.text

    out, err = capsys.readouterr()

    # validate metavars are all in lowercase
    usage = "\n".join(takewhile(lambda o: bool(o), out.splitlines()))

    message = (
        "metavars not lowercased, you are likely missing formatter_class=XXX "
        "in the command, where XXX should be any of the classes from "
        "`dvc.cli.formatter`, which automatically lowercases metavars.\n"
        "\nExample:\n"
        "\nfrom dvc.cli import formatter\n"
        "\nparser.add_parser(..., formatter_class=formatter.TextHelpFormatter)\n"
    )
    assert not re.findall(r"\b[A-Z]{2,}\b", usage, re.MULTILINE), message

    assert not err
    assert out
