def test_repro(bench_dvc, tmp_dir, dvc, dataset):
    dvc.add(dataset)
    source_path = dataset.relative_to(tmp_dir).as_posix()
    dvc.stage.add(
        name="copy-1",
        deps=[source_path],
        outs=["copy-1"],
        cmd=f"cp -R {source_path} copy-1",
    )
    dvc.stage.add(
        name="copy-2",
        deps=[source_path],
        outs=["copy-2"],
        cmd=f"cp -R {source_path} copy-2",
    )
    combine_cmds = [
        "mkdir combine",
        "cp -R copy-1 combine/1",
        "cp -R copy-2 combine/2",
    ]
    dvc.stage.add(
        name="combine", deps=["copy-1", "copy-2"], outs=["combine"], cmd=combine_cmds
    )
    bench_dvc("repro")
    bench_dvc("repro", name="noop")
