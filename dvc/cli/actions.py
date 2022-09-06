from argparse import _AppendAction


class KeyValueArgs(_AppendAction):
    def __call__(self, parser, namespace, values, option_string=None):
        items = getattr(namespace, self.dest) or {}
        for value in filter(bool, values):
            key, _, value = value.partition("=")
            items[key.strip()] = value

        setattr(namespace, self.dest, items)
