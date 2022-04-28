from dvc.fs import GSFileSystem


def test_gs_trust_env():
    gs = GSFileSystem()
    session = gs.fs._session
    assert session.trust_env
