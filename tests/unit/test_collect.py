import logging
import os

from dvc.repo.collect import collect


def test_no_file_on_target_rev(tmp_dir, scm, dvc, caplog):
    with caplog.at_level(logging.WARNING, "dvc"):
        collect(dvc, targets=["file.yaml"], rev="current_branch")

    assert "'file.yaml' was not found at: 'current_branch'." in caplog.text


def test_collect_order(tmp_dir, scm, dvc, run_copy):
    def run_cp(input_path, destinations):
        cmd = " && ".join(f"cp {input_path} {dst}" for dst in destinations)
        name = "-".join(destinations)
        dvc.run(cmd=cmd, name=name, metrics=destinations, deps=[input_path])

    def out_filter(out):
        return bool(out.metric)

    tmp_dir.dvc_gen("input", "v: 1", commit="add data")

    run_cp("input", ["1", "2"])
    run_cp("input", ["3"])

    tmp_dir.gen({"sub": {}})
    with (tmp_dir / "sub").chdir():
        input_path = os.path.join("..", "input")
        run_cp(input_path, ["1", "2"])
        run_cp(input_path, ["3"])

    outs, _ = collect(dvc, output_filter=out_filter)

    assert list(map(str, outs)) == [
        "1",
        "2",
        "3",
        os.path.join("sub", "1"),
        os.path.join("sub", "2"),
        os.path.join("sub", "3"),
    ]
