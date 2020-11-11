from textwrap import dedent

DVCLIVE_SCRITP = dedent(
    """\
        from dvclive import dvclive
        import sys
        from ruamel import yaml

        with open("params.yaml", "r") as fd:
            params = yaml.load(fd)

        multiplier = float(params["multiplier"])

        dvclive.init("{log_path}")
        for i in range(10):
           dvclive.log("loss", 2**(1/(i+1)/multiplier))
           dvclive.log("accuracy", i/10*multiplier)
           dvclive.next_epoch()
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
        dvclive=["logs"],
    )
    scm.add(["params.yaml", "log.py", "dvc.lock", "dvc.yaml", "logs"])
    scm.commit("init")

    scm.checkout("improved", create_new=True)
    tmp_dir.gen("params.yaml", "multiplier: 1.4")
    dvc.reproduce("run_logger")
