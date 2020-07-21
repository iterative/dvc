import os

from funcy import collecting


@collecting
def find(tree, top=None):
    print("tree, top, tree_root: ", tree, top, tree.tree_root)
    top = top or tree.tree_root
    for root, _, _ in tree.walk(os.path.abspath(top)):
        print("root: ", root)
        print("checking if it's a DVC repo", os.path.join(root, ".dvc"), tree.isdir(os.path.join(root, ".dvc")))
        if tree.isdir(os.path.join(root, ".dvc")):
            yield root
