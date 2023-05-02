from argparse import _AppendAction


class CommaSeparatedArgs(_AppendAction):  # pylint: disable=protected-access
    def __call__(self, parser, namespace, values, option_string=None):  # noqa: ARG002
        from funcy import ldistinct

        items = getattr(namespace, self.dest) or []
        items.extend(map(str.strip, values.split(",")))
        setattr(namespace, self.dest, ldistinct(items))


class KeyValueArgs(_AppendAction):
    def __call__(self, parser, namespace, values, option_string=None):  # noqa: ARG002
        items = getattr(namespace, self.dest) or {}
        for value in filter(bool, values):
            key, _, value = value.partition("=")
            items[key.strip()] = value

        setattr(namespace, self.dest, items)
