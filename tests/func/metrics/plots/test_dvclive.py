import shutil
from textwrap import dedent

DVCLIVE_SCRITP = dedent(
    """
        from dvclive import dvclive
        import sys
        
        for i in range(5):
           dvclive.log("loss", -i/5)
           dvclive.log("accuracy", i/5)"""
)


def test_export_config():
    # check "DVCLIVE_CONFIG"
    pass


def test_summary_is_metric(tmp_dir, dvc):
    tmp_dir.gen("log.py", DVCLIVE_SCRITP.format(log_path="logs"))
    dvc.run(
        cmd="python log.py",
        deps=["log.py"],
        name="run_logger",
        dvclive=["logs"],
    )

    assert dvc.metrics.show() == {
        "": {"logs.json": {"step": 3, "loss": -0.6, "accuracy": 0.6}}
    }

    plots = dvc.plots.show()
    assert "logs/accuracy.tsv" in plots
    assert "logs/loss.tsv" in plots


def test_live_plots(tmp_dir, scm, dvc):
    tmp_dir.gen("log.py", DVCLIVE_SCRITP.format(log_path="logs"))
    dvc.run(
        cmd="python log.py",
        deps=["log.py"],
        name="run_logger",
        dvclive=["logs"],
    )
    shutil.rmtree(".dvc/cache")
    shutil.rmtree("logs")

    dvc.metrics.show()
    pass
    # scm.add(["params.yaml", "log.py", "dvc.lock", "dvc.yaml", "logs"])
    # scm.commit("init")
    #
    # scm.checkout("improved", create_new=True)
    # tmp_dir.gen("params.yaml", "multiplier: 1.4")
    # dvc.reproduce("run_logger")
    #
    # assert (tmp_dir / "logs").is_dir()
    # assert (tmp_dir / "logs.json").is_file()
    #
    # assert main(["plots", "diff", "master"]) == 0

    # whole dir seems to not be working
    # assert main(["plots", "diff", "master", "--targets", "logs/history"]) ==
    # 0
    # assert (
    #     main(["plots", "diff", "master", "--targets", "logs/accuracy.tsv"])
    #     == 0
    # "--dvclive",
    #             "dvclive")
