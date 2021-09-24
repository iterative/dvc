import os

from dvc.command.experiments import CmdExperimentsInit
from dvc.main import main
from dvc.utils.serialize import load_yaml


def test_init(tmp_dir, dvc):
    tmp_dir.gen(
        {
            CmdExperimentsInit.CODE: {"copy.py": ""},
            "data": "data",
            "params.yaml": '{"foo": 1}',
            "dvclive": {},
            "plots": {},
        }
    )
    code_path = os.path.join(CmdExperimentsInit.CODE, "copy.py")
    script = f"python {code_path}"

    assert main(["exp", "init", script]) == 0
    assert load_yaml(tmp_dir / "dvc.yaml") == {
        "stages": {
            "default": {
                "cmd": script,
                "deps": ["data", "src"],
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": ["models"],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }
