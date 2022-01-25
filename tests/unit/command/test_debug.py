from dvc.cli import main


def test_viztracer(tmp_dir, dvc, mocker):
    viztracer_profile = mocker.patch("dvc._debug.viztracer_profile")

    assert main(["status", "--viztracer"]) == 0

    args = viztracer_profile.call_args[1]
    assert callable(args["path"])
    assert args["max_stack_depth"] == -1

    assert main(["status", "--viztracer", "--viztracer-depth", "5"]) == 0
    args = viztracer_profile.call_args[1]
    assert args["max_stack_depth"] == 5

    assert main(["status", "--viztracer-depth", "2"]) == 0
    args = viztracer_profile.call_args[1]
    assert args["max_stack_depth"] == 2


def test_viztracer_internal(tmp_dir, dvc, mocker):
    import sys

    viztracer = mocker.MagicMock()
    sys.modules["viztracer"] = viztracer
    from dvc import _debug

    profile = mocker.spy(_debug, "viztracer_profile")
    tracer = viztracer.VizTracer.return_value
    assert main(["status", "--viztracer"]) == 0

    args = viztracer.VizTracer.call_args[1]
    assert args["max_stack_depth"] == -1

    tracer.start.assert_called_once()
    tracer.stop.assert_called_once()
    tracer.save.assert_called_once_with(profile.call_args[1]["path"]())
