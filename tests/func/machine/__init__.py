import textwrap

CONFIG_TEXT = textwrap.dedent(
    """\
        [feature]
            machine = true
        ['machine "foo"']
            cloud = aws
    """
)
