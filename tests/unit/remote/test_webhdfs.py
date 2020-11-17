from dvc.tree.webhdfs import WebHDFSTree

user = "test"
webhdfs_token = "token"
webhdfs_alias = "alias-name"
hdfscli_config = "path/to/cli/config"


def test_init(dvc):
    url = "webhdfs://test@127.0.0.1:50070"
    config = {
        "url": url,
        "webhdfs_token": webhdfs_token,
        "webhdfs_alias": webhdfs_alias,
        "hdfscli_config": hdfscli_config,
        "user": user,
    }

    tree = WebHDFSTree(dvc, config)
    assert tree.path_info == url
    assert tree.token == webhdfs_token
    assert tree.alias == webhdfs_alias
    assert tree.path_info.user == user
    assert tree.hdfscli_config == hdfscli_config
