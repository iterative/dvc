import os

from funcy import collecting


@collecting
def find(tree, top=None):
    top = top or tree.tree_root
    for root, _, _ in tree.walk(top):
        if tree.isdir(os.path.join(root, ".dvc")):
            yield root
