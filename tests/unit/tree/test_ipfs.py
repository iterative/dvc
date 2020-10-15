from dvc.tree import IPFSTree


def test_init(dvc):
    url = "ipfs://TODO"
    config = {"url": url}
    tree = IPFSTree(dvc, config)

    assert tree.path_info == url
