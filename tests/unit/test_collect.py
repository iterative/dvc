from dvc.repo.collect import collect


def test_collect_duplicates(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "foo: 1\nbar: 2")
    tmp_dir.gen("foobar", "")

    dvc.run(name="stage-1", cmd="echo stage-1", params=["foo"])
    dvc.run(name="stage-2", cmd="echo stage-2", params=["bar"])

    outs, _ = collect(dvc, deps=True, targets=["params.yaml"])
    assert len(outs) == 1

    outs, _ = collect(dvc, deps=True, targets=["params.yaml"], duplicates=True)
    assert len(outs) == 2

    outs, _ = collect(dvc, deps=True, targets=["foobar"], duplicates=True)
    assert not outs
