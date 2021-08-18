PARAMETRIZED_DVC_YAML = """\
stages:
  stage1:
    cmd: cp ${foo} ${bar}
    deps:
    - ${foo}
    outs:
    - ${bar}
  stage2:
    foreach: ${mapping}
    do:
      cmd: cp ${item.from} ${item.to}
      deps:
      - ${item.from}
      outs:
      - ${item.to}
"""

PARAMS_YAML = """\
foo: "random_string"
bar: "random_string2"
mapping:
  first:
    from: lorem
    to: ipsum
  second:
    from: foo
    to: foobar
  third:
    from: hello
    to: world
  fourth:
    from: one
    to: two
"""


def test_stage_list(tmp_dir, dvc, dvc_cmd, benchmark):
    (tmp_dir / "dvc.yaml").write_text(PARAMETRIZED_DVC_YAML)
    (tmp_dir / "params.yaml").write_text(PARAMS_YAML)

    stage_add = dvc_cmd.args("stage", "list")
    result = benchmark(stage_add)

    assert result.returncode == 0
    assert not result.stderr
    out = [item.split()[0] for item in result.stdout.splitlines()]
    assert out == [
        "stage1",
        "stage2@first",
        "stage2@second",
        "stage2@third",
        "stage2@fourth",
    ]
