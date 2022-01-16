from pluggy import HookimplMarker, HookspecMarker

hookspec = HookspecMarker("dvc")
hookimpl = HookimplMarker("dvc")


@hookspec
def register_command(parser, parent):  # pylint: disable=unused-argument
    pass
