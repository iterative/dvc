def test_number_reproduces(tmp_dir, dvc, mocker):
    reproduce_stage_mock = mocker.patch(
        "dvc.repo.reproduce._reproduce_stage", returns=[]
    )
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.run(name="echo-foo", outs=["foo"], cmd="echo foo > foo")
    dvc.run(name="echo-bar", deps=["foo"], outs=["bar"], cmd="echo bar > bar")
    dvc.run(name="echo-baz", deps=["foo"], outs=["baz"], cmd="echo baz > baz")
    dvc.run(name="echo-boop", deps=["bar"], outs=["boop"], cmd="echo boop > boop")

    reproduce_stage_mock.reset_mock()

    dvc.reproduce(all_pipelines=True)

    assert reproduce_stage_mock.call_count == 5
