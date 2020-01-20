from dvc.config import Config


def test_remote_config_no_traverse():
    d = Config.COMPILED_SCHEMA({'remote "myremote"': {"url": "url"}})
    assert "no_traverse" not in d['remote "myremote"']

    d = Config.COMPILED_SCHEMA(
        {'remote "myremote"': {"url": "url", "no_traverse": "fAlSe"}}
    )
    assert not d['remote "myremote"']["no_traverse"]

    d = Config.COMPILED_SCHEMA(
        {'remote "myremote"': {"url": "url", "no_traverse": "tRuE"}}
    )
    assert d['remote "myremote"']["no_traverse"]
