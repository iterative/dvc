import os
from funcy import post_processing


@post_processing("\r\n".join)
def visualize(tree, top, **kwargs):
    """`tree`-like output, useful for debugging/visualizing, needs `walk()`"""
    indent = 4
    spacing = " " * indent
    tee = "├── "
    last = "└── "
    for root, _, files in tree.walk(top, **kwargs):
        level = root.replace(top, "").count(os.sep)
        indent = spacing * level
        yield "{}{}/".format(indent, os.path.basename(root))
        sub_indent = spacing * (level + 1)
        length = len(files)
        for i, f in enumerate(files):
            yield "{}{}{}".format(
                sub_indent, tee if i + 1 != length else last, f
            )
