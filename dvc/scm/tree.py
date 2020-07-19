def is_working_tree(tree):
    from dvc.tree.local import LocalRemoteTree

    return isinstance(tree, LocalRemoteTree) or isinstance(
        getattr(tree, "tree", None), LocalRemoteTree
    )
