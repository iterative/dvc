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
