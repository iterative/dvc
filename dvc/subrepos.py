import os

from funcy import collecting


@collecting
def find(tree, top=None, skip_top_level=True):
    top = top or tree.tree_root
    for root, _, _ in tree.walk(top):
        if skip_top_level and top == root:
            continue
        if tree.isdir(os.path.join(root, ".dvc")):
            yield root
