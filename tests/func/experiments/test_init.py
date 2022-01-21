import io
import os

import pytest

from dvc.cli import main
from dvc.commands.experiments.init import CmdExperimentsInit
from dvc.exceptions import DvcException
from dvc.repo.experiments.init import init
from dvc.stage.exceptions import DuplicateStageName

# the tests may hang on prompts on failure
pytestmark = pytest.mark.timeout(3, func_only=True)


@pytest.mark.timeout(5, func_only=True)
def test_init_simple(tmp_dir, scm, dvc, capsys):
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

    capsys.readouterr()
    assert main(["exp", "init", script]) == 0
    out, err = capsys.readouterr()
    assert not err
    assert "Created train stage in dvc.yaml" in out
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": script,
                "deps": ["data", "src"],
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": ["models"],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }


@pytest.mark.parametrize("interactive", [True, False])
def test_when_stage_already_exists_with_same_name(tmp_dir, dvc, interactive):
    (tmp_dir / "dvc.yaml").dump({"stages": {"train": {"cmd": "test"}}})
    with pytest.raises(DuplicateStageName) as exc:
        init(
            dvc,
            interactive=interactive,
            overrides={"cmd": "true"},
            defaults=CmdExperimentsInit.DEFAULTS,
        )
    assert (
        str(exc.value) == "Stage 'train' already exists in 'dvc.yaml'. "
        "Use '--force' to overwrite."
    )


def test_when_stage_force_if_already_exists(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump({"foo": 1})
    (tmp_dir / "dvc.yaml").dump({"stages": {"train": {"cmd": "test"}}})
    init(
        dvc,
        force=True,
        overrides={"cmd": "true"},
        defaults=CmdExperimentsInit.DEFAULTS,
    )
    d = (tmp_dir / "dvc.yaml").parse()
    assert d["stages"]["train"]["cmd"] == "true"


def test_with_a_custom_name(tmp_dir, dvc):
    init(dvc, name="custom", overrides={"cmd": "cmd"})
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {"custom": {"cmd": "cmd"}}
    }


def test_init_with_no_defaults_non_interactive(tmp_dir, scm, dvc):
    init(dvc, defaults={}, overrides={"cmd": "python script.py"})

    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {"train": {"cmd": "python script.py"}}
    }
    scm._reset()
    assert not (tmp_dir / "dvc.lock").exists()
    assert scm.is_tracked("dvc.yaml")


def test_abort_confirmation(tmp_dir, dvc):
    (tmp_dir / "param").dump({"foo": 1})
    inp = io.StringIO("./script\nscript\ndata\nmodel\nparam\nmetric\nplt\nn")
    with pytest.raises(DvcException) as exc:
        init(
            dvc,
            interactive=True,
            defaults=CmdExperimentsInit.DEFAULTS,
            stream=inp,
        )
    assert str(exc.value) == "Aborting ..."
    assert not (tmp_dir / "dvc.yaml").exists()
    assert not (tmp_dir / "dvc.lock").exists()


@pytest.mark.parametrize(
    "extra_overrides, inp",
    [
        ({"cmd": "cmd"}, io.StringIO()),
        ({}, io.StringIO("cmd")),
    ],
)
def test_init_interactive_when_no_path_prompts_need_to_be_asked(
    tmp_dir, dvc, extra_overrides, inp
):
    """When we pass everything that's required of, it should not prompt us."""
    (tmp_dir / "params.yaml").dump({"foo": 1})
    init(
        dvc,
        interactive=True,
        defaults=CmdExperimentsInit.DEFAULTS,
        overrides={**CmdExperimentsInit.DEFAULTS, **extra_overrides},
        stream=inp,  # we still need to confirm
    )
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "cmd",
                "deps": ["data", "src"],
                "live": {"dvclive": {"html": True, "summary": True}},
                "metrics": [{"metrics.json": {"cache": False}}],
                # we specify `live` through `overrides`,
                # so it creates checkpoint-based output.
                "outs": [{"models": {"checkpoint": True}}],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }


def test_when_params_is_omitted_in_interactive_mode(tmp_dir, scm, dvc):
    (tmp_dir / "params.yaml").dump({"foo": 1})
    inp = io.StringIO("python script.py\nscript.py\ndata\nmodels\nn")

    init(
        dvc, interactive=True, stream=inp, defaults=CmdExperimentsInit.DEFAULTS
    )

    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "python script.py",
                "deps": ["data", "script.py"],
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": ["models"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }
    assert not (tmp_dir / "dvc.lock").exists()
    scm._reset()
    assert scm.is_tracked("dvc.yaml")
    assert not scm.is_tracked("params.yaml")
    assert scm.is_tracked(".gitignore")
    assert scm.is_ignored("models")


def test_init_interactive_params_validation(tmp_dir, dvc, capsys):
    tmp_dir.gen({"data": {"foo": "foo"}})
    (tmp_dir / "params.yaml").dump({"foo": 1})
    inp = io.StringIO(
        "python script.py\nscript.py\ndata\nmodels\nparams.json\ndata\n"
    )

    init(
        dvc, stream=inp, interactive=True, defaults=CmdExperimentsInit.DEFAULTS
    )

    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "python script.py",
                "deps": ["data", "script.py"],
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": ["models"],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }

    out, err = capsys.readouterr()
    assert (
        "Path to a parameters file [params.yaml, n to omit]: "
        "'params.json' does not exist. "
        "Please retry with an existing parameters file.\n"
        "Path to a parameters file [params.yaml, n to omit]: "
        "'data' is a directory. "
        "Please retry with an existing parameters file.\n"
        "Path to a parameters file [params.yaml, n to omit]:"
    ) in err
    assert not out


def test_init_with_no_defaults_interactive(tmp_dir, dvc):
    inp = io.StringIO(
        "python script.py\n"
        "script.py\n"
        "data\n"
        "model\n"
        "n\n"
        "metric\n"
        "n\n"
    )
    init(
        dvc,
        defaults={},
        overrides={"cmd": "python script.py"},
        interactive=True,
        stream=inp,
    )
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "python script.py",
                "deps": ["python script.py", "script.py"],
                "metrics": [{"metric": {"cache": False}}],
                "outs": ["data"],
            }
        }
    }


@pytest.mark.parametrize(
    "interactive, overrides, inp",
    [
        (False, {"cmd": "python script.py", "code": "script.py"}, None),
        (
            True,
            {},
            io.StringIO(
                "python script.py\n"
                "script.py\n"
                "data\n"
                "models\n"
                "params.yaml\n"
                "metrics.json\n"
                "plots\n"
                "y"
            ),
        ),
    ],
    ids=["non-interactive", "interactive"],
)
def test_init_interactive_default(
    tmp_dir, scm, dvc, interactive, overrides, inp, capsys
):
    (tmp_dir / "params.yaml").dump({"foo": {"bar": 1}})

    init(
        dvc,
        interactive=interactive,
        defaults=CmdExperimentsInit.DEFAULTS,
        overrides=overrides,
        stream=inp,
    )

    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "python script.py",
                "deps": ["data", "script.py"],
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": ["models"],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }
    assert not (tmp_dir / "dvc.lock").exists()
    scm._reset()
    assert scm.is_tracked("dvc.yaml")
    assert scm.is_tracked("params.yaml")
    assert scm.is_tracked(".gitignore")
    assert scm.is_ignored("models")
    out, err = capsys.readouterr()

    if interactive:
        assert "'script.py' does not exist in the workspace." in err
        assert "'data' does not exist in the workspace." in err
    assert not out


@pytest.mark.timeout(5, func_only=True)
@pytest.mark.parametrize(
    "interactive, overrides, inp",
    [
        (False, {"cmd": "python script.py", "code": "script.py"}, None),
        (
            True,
            {},
            io.StringIO(
                "python script.py\n"
                "script.py\n"
                "data\n"
                "models\n"
                "params.yaml\n"
                "dvclive\n"
                "y"
            ),
        ),
        (
            True,
            {"cmd": "python script.py"},
            io.StringIO(
                "script.py\n"
                "data\n"
                "models\n"
                "params.yaml\n"
                "dvclive\n"
                "y"
            ),
        ),
        (
            True,
            {"cmd": "python script.py", "models": "models"},
            io.StringIO("script.py\ndata\nparams.yaml\ndvclive\ny"),
        ),
    ],
    ids=[
        "non-interactive",
        "interactive",
        "interactive-cmd-provided",
        "interactive-cmd-models-provided",
    ],
)
def test_init_interactive_live(
    tmp_dir, scm, dvc, interactive, overrides, inp, capsys
):
    (tmp_dir / "params.yaml").dump({"foo": {"bar": 1}})

    init(
        dvc,
        type="dl",
        interactive=interactive,
        defaults=CmdExperimentsInit.DEFAULTS,
        overrides=overrides,
        stream=inp,
    )
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "python script.py",
                "deps": ["data", "script.py"],
                "live": {"dvclive": {"html": True, "summary": True}},
                "outs": [{"models": {"checkpoint": True}}],
                "params": ["foo"],
            }
        }
    }
    assert not (tmp_dir / "dvc.lock").exists()
    scm._reset()
    assert scm.is_tracked("dvc.yaml")
    assert scm.is_tracked("params.yaml")
    assert scm.is_tracked(".gitignore")
    assert scm.is_ignored("models")

    out, err = capsys.readouterr()
    if interactive:
        assert "'script.py' does not exist in the workspace." in err
        assert "'data' does not exist in the workspace." in err
    assert not out


@pytest.mark.parametrize(
    "interactive, inp",
    [
        (False, None),
        (True, io.StringIO()),
    ],
)
def test_init_with_type_live_and_models_plots_provided(
    tmp_dir, dvc, interactive, inp
):
    (tmp_dir / "params.yaml").dump({"foo": 1})
    init(
        dvc,
        type="dl",
        interactive=interactive,
        stream=inp,
        defaults=CmdExperimentsInit.DEFAULTS,
        overrides={"cmd": "cmd", "metrics": "m", "plots": "p"},
    )
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "cmd",
                "deps": ["data", "src"],
                "live": {"dvclive": {"html": True, "summary": True}},
                "metrics": [{"m": {"cache": False}}],
                "outs": [{"models": {"checkpoint": True}}],
                "params": ["foo"],
                "plots": [{"p": {"cache": False}}],
            }
        }
    }


@pytest.mark.parametrize(
    "interactive, inp",
    [
        (False, None),
        (True, io.StringIO()),
    ],
)
def test_init_with_type_default_and_live_provided(
    tmp_dir, dvc, interactive, inp
):
    (tmp_dir / "params.yaml").dump({"foo": 1})
    init(
        dvc,
        interactive=interactive,
        stream=inp,
        defaults=CmdExperimentsInit.DEFAULTS,
        overrides={"cmd": "cmd", "live": "live"},
    )
    assert (tmp_dir / "dvc.yaml").parse() == {
        "stages": {
            "train": {
                "cmd": "cmd",
                "deps": ["data", "src"],
                "live": {"live": {"html": True, "summary": True}},
                "metrics": [{"metrics.json": {"cache": False}}],
                "outs": [{"models": {"checkpoint": True}}],
                "params": ["foo"],
                "plots": [{"plots": {"cache": False}}],
            }
        }
    }
