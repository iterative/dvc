import os

import pytest

import dvc.output as dvc_output


def test_stage_cache(tmp_dir, dvc, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen("script.py", 'open("out", "w+").write("out"); ')
    stage = dvc.run(
        cmd="python script.py",
        deps=["script.py", "dep"],
        outs=["out"],
        name="write-out",
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "dvc.yaml").exists()
    assert not (tmp_dir / "dvc.lock").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "c7",
        "c7a85d71de1912c43d9c5b6218c71630d6277e8fc11e4ccf5e12b3c202234838",
    )
    cache_file = os.path.join(
        cache_dir, "5e2824029f8da9ef6c57131a638ceb65f27c02f16c85d71e85671c27daed0501"
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 2

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out").read_text() == "out"


def test_stage_cache_params(tmp_dir, dvc, mocker):
    tmp_dir.gen("params.yaml", "foo: 1\nbar: 2")
    tmp_dir.gen("myparams.yaml", "baz: 3\nqux: 4")
    tmp_dir.gen("script.py", 'open("out", "w+").write("out"); ')
    stage = dvc.run(
        cmd="python script.py",
        params=["foo,bar", "myparams.yaml:baz,qux"],
        outs=["out"],
        name="write-out",
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "dvc.yaml").exists()
    assert not (tmp_dir / "dvc.lock").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "8f",
        "8fdb377d1b4c0a303b788771b122dfba9bbbbc43f14ce41d35715cf4fea08459",
    )
    cache_file = os.path.join(
        cache_dir, "9ce1963a69beb1299800188647cd960b7afff101be19fd46226e32bb8be8ee44"
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 2

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out").read_text() == "out"


def test_stage_cache_wdir(tmp_dir, dvc, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen("script.py", 'open("out", "w+").write("out"); ')
    tmp_dir.gen({"wdir": {}})
    stage = dvc.run(
        cmd="python ../script.py",
        deps=["../script.py", "../dep"],
        outs=["out"],
        name="write-out",
        wdir="wdir",
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "wdir" / "out").exists()
    assert not (tmp_dir / "wdir" / "dvc.yaml").exists()
    assert not (tmp_dir / "wdir" / "dvc.lock").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "6a",
        "6ad5cce3347e9e96c77d4353d84e5c8cae8c9151c486c4ea3d3d79e9051800f1",
    )
    cache_file = os.path.join(
        cache_dir, "1fa3e1a9f785f8364ad185d62bd0813ce8794afcfc79410e985eac0443cc5462"
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(dvc_output, "checkout")
    with dvc.lock:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 2

    assert (tmp_dir / "wdir" / "out").exists()
    assert (tmp_dir / "wdir" / "out").read_text() == "out"


def test_shared_stage_cache(tmp_dir, dvc, run_copy):
    import stat

    from dvc.cachemgr import CacheManager

    tmp_dir.gen("foo", "foo")

    with dvc.config.edit() as config:
        config["cache"]["shared"] = "group"

    dvc.cache = CacheManager(dvc)

    assert not os.path.exists(dvc.cache.local.path)

    run_copy("foo", "bar", name="copy-foo-bar")

    parent_cache_dir = os.path.join(dvc.stage_cache.cache_dir, "6d")
    cache_dir = os.path.join(
        parent_cache_dir,
        "6d4c6de74e7c0d60d2122e2063b4724a8c78a6799def6c7cf5093f45e7f2f3b7",
    )
    cache_file = os.path.join(
        cache_dir, "435e34f80692059e79ec53346cbfe29fd9ee65b62f5f06f17f5950ce5a7408ea"
    )

    # sanity check
    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    def _mode(path):
        return stat.S_IMODE(os.stat(path).st_mode)

    if os.name == "nt":
        dir_mode = 0o777
        file_mode = 0o666
    else:
        dir_mode = 0o2775
        file_mode = 0o664

    assert _mode(dvc.cache.local.path) == dir_mode
    assert _mode(dvc.stage_cache.cache_dir) == dir_mode
    assert _mode(parent_cache_dir) == dir_mode
    assert _mode(cache_dir) == dir_mode
    assert _mode(cache_file) == file_mode


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"cmd": "cmd"},
        {"cmd": "cmd", "deps": ["path"]},
        {"cmd": "cmd", "outs": ["path"]},
        {"always_changed": True},
    ],
)
def test_unhashable(tmp_dir, dvc, mocker, kwargs):
    from dvc.stage import Stage, create_stage
    from dvc.stage.cache import RunCacheNotFoundError, StageCache

    cache = StageCache(dvc)
    stage = create_stage(Stage, path="stage.dvc", repo=dvc, **kwargs)
    get_stage_hash = mocker.patch("dvc.stage.cache._get_stage_hash")
    assert cache.save(stage) is None
    get_stage_hash.assert_not_called()
    with pytest.raises(RunCacheNotFoundError):
        cache.restore(stage)
    get_stage_hash.assert_not_called()
