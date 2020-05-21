import os


def test_stage_cache(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("dep", "dep")
    tmp_dir.gen(
        "script.py",
        (
            'open("out", "w+").write("out"); '
            'open("out_no_cache", "w+").write("out_no_cache")'
        ),
    )
    stage = dvc.run(
        cmd="python script.py",
        deps=["script.py", "dep"],
        outs=["out"],
        outs_no_cache=["out_no_cache"],
        single_stage=True,
    )

    with dvc.lock, dvc.state:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out_no_cache").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "10",
        "10b45372fdf4ec14d3f779c5b256378d7a12780e4c7f549a44138e492f098bfe",
    )
    cache_file = os.path.join(
        cache_dir,
        "bb32e04c6da96a7192513390acedbe4cd6123f8fe5b0ba5fffe39716fe87f6f4",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(stage, "checkout")
    with dvc.lock, dvc.state:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 1

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out_no_cache").exists()
    assert (tmp_dir / "out").read_text() == "out"
    assert (tmp_dir / "out_no_cache").read_text() == "out_no_cache"


def test_stage_cache_params(tmp_dir, dvc, run_copy, mocker):
    tmp_dir.gen("params.yaml", "foo: 1\nbar: 2")
    tmp_dir.gen("myparams.yaml", "baz: 3\nqux: 4")
    tmp_dir.gen(
        "script.py",
        (
            'open("out", "w+").write("out"); '
            'open("out_no_cache", "w+").write("out_no_cache")'
        ),
    )
    stage = dvc.run(
        cmd="python script.py",
        params=["foo,bar", "myparams.yaml:baz,qux"],
        outs=["out"],
        outs_no_cache=["out_no_cache"],
        single_stage=True,
    )

    with dvc.lock, dvc.state:
        stage.remove(remove_outs=True, force=True)

    assert not (tmp_dir / "out").exists()
    assert not (tmp_dir / "out_no_cache").exists()
    assert not (tmp_dir / "out.dvc").exists()

    cache_dir = os.path.join(
        dvc.stage_cache.cache_dir,
        "65",
        "651d0a5b82e05e48b03acf44954f6a8599760e652a143d517a17d1065eca61a1",
    )
    cache_file = os.path.join(
        cache_dir,
        "2196a5a4dd24c5759437511fcf9d6aa66b259e1dac58e3f212aefd1797a6f114",
    )

    assert os.path.isdir(cache_dir)
    assert os.listdir(cache_dir) == [os.path.basename(cache_file)]
    assert os.path.isfile(cache_file)

    run_spy = mocker.patch("dvc.stage.run.cmd_run")
    checkout_spy = mocker.spy(stage, "checkout")
    with dvc.lock, dvc.state:
        stage.run()

    assert not run_spy.called
    assert checkout_spy.call_count == 1

    assert (tmp_dir / "out").exists()
    assert (tmp_dir / "out_no_cache").exists()
    assert (tmp_dir / "out").read_text() == "out"
    assert (tmp_dir / "out_no_cache").read_text() == "out_no_cache"
