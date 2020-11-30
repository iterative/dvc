from textwrap import dedent

from dvc.main import main

DVCLIVE_SCRITP = dedent(
    """\
        from dvclive import dvclive
        import sys
        from ruamel import yaml

        with open("params.yaml", "r") as fd:
            params = yaml.load(fd)

        multiplier = float(params["multiplier"])

        dvclive.init("{log_path}")
        for i in range(5):
           dvclive.log("loss", -i/5/multiplier)
           dvclive.log("accuracy", i/5*multiplier)
           dvclive.next_step()
"""
)


def test_live_plots(tmp_dir, scm, dvc):
    tmp_dir.gen("params.yaml", "multiplier: 1")
    tmp_dir.gen("log.py", DVCLIVE_SCRITP.format(log_path="logs"))
    dvc.run(
        cmd="python log.py",
        deps=["log.py"],
        params=["multiplier"],
        name="run_logger",
        plots_no_cache=["logs"],
        metrics_no_cache=["logs.json"],
    )
    scm.add(["params.yaml", "log.py", "dvc.lock", "dvc.yaml", "logs"])
    scm.commit("init")

    scm.checkout("improved", create_new=True)
    tmp_dir.gen("params.yaml", "multiplier: 1.4")
    dvc.reproduce("run_logger")

    assert (tmp_dir / "logs").is_dir()
    assert (tmp_dir / "logs.json").is_file()

    assert main(["plots", "diff", "master"]) == 0

    # whole dir seems to not be working
    # assert main(["plots", "diff", "master", "--targets", "logs/history"]) ==
    # 0
    assert (
        main(["plots", "diff", "master", "--targets", "logs/accuracy.tsv"])
        == 0
    )
