import os

from dvc.main import main


def test_init(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "src": {"copy.py": ""},
            "data": "data",
            "params.yaml": '{"foo": 1}',
            "dvclive": {},
            "plots": {},
        }
    )
    code_path = os.path.join("src", "copy.py")
    script = f"python {code_path}"

    assert main(["exp", "init", script]) == 0
    assert (tmp_dir / "dvc.yaml").parse() == {
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


def test_init_live(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "src": {"copy.py": ""},
            "data": "data",
            "params.yaml": '{"foo": 1}',
            "dvclive": {},
            "plots": {},
        }
    )
    code_path = os.path.join("src", "copy.py")
    script = f"python {code_path}"

    assert main(["exp", "init", "--template", "live", script]) == 0
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "live": {
                "cmd": script,
                "deps": ["data", "src"],
                "outs": ["models"],
                "params": ["foo"],
                "live": {"dvclive": {"html": True, "summary": True}},
            }
        }
    }
