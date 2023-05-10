import os

import pytest

import dvc.output as dvc_output


def test_stage_cache(tmp_dir, dvc, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen(
        "script.py",
        'open("out", "w+").write("out"); ',
    )
    stage = dvc.run(
        cmd="python script.py",
        deps=["script.py", "dep"],
        outs=["out"],
        single_stage=True,
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "4b",
        "4b495dc2b14e1ca5bd5f2765a99ddd8785523a8342efe6bcadac012c2db01165",
    )
    cache_file = os.path.join(
        cache_dir,
        "78c427104a8e20216031c36d9c7620733066fff33ada254ad3fca551c1f8152b",
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
    tmp_dir.gen(
        "script.py",
        'open("out", "w+").write("out"); ',
    )
    stage = dvc.run(
        cmd="python script.py",
        params=["foo,bar", "myparams.yaml:baz,qux"],
        outs=["out"],
        single_stage=True,
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "8f",
        "8fdb377d1b4c0a303b788771b122dfba9bbbbc43f14ce41d35715cf4fea08459",
    )
    cache_file = os.path.join(
        cache_dir,
        "202ea269108bf98bea3e15f978e7929864728956c9df8d927a5c7d74fc4fedc8",
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
    tmp_dir.gen(
        "script.py",
        'open("out", "w+").write("out"); ',
    )
    tmp_dir.gen({"wdir": {}})
    stage = dvc.run(
        cmd="python ../script.py",
        deps=["../script.py", "../dep"],
        outs=["out"],
        single_stage=True,
        wdir="wdir",
    )

    with dvc.lock:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "wdir" / "out").exists()
    assert not (tmp_dir / "wdir" / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "b5",
        "b5d5548c43725139aa3419eb50717e062fe9b81f866f401fd6fd778d2a97822d",
    )
    cache_file = os.path.join(
        cache_dir,
        "e0a59f6a5bd193032a4d1500abd89c9b08a2e9b26c724015e0bc6374295a3b9f",
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

    parent_cache_dir = os.path.join(dvc.stage_cache.cache_dir, "fd")
    cache_dir = os.path.join(
        parent_cache_dir,
        "fdbe7847136ebe88f59a29ee5a568f893cab031f74f7d3ab050828b53fd0033a",
    )
    cache_file = os.path.join(
        cache_dir,
        "8716052b2074f5acf1a379529d47d6ef1c1f50c2281a7489b8d7ce251f234f86",
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
    assert get_stage_hash.not_called
    with pytest.raises(RunCacheNotFoundError):
        cache.restore(stage)
    assert get_stage_hash.not_called
