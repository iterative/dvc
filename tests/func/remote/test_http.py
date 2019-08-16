from dvc.main import main


def test_incompatibility_with_no_traverse(dvc_repo, caplog):
    main(["remote", "add", "http", "https://example.com"])

    assert 0 != main(["status", "--remote", "http"])

    assert (
        "RemoteHTTP does not support 'no_traverse' option. "
        "Disable it with `dvc remote modify <name> no_traverse false`"
    ) in caplog.text
