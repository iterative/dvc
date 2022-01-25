from contextlib import ExitStack, contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Union

if TYPE_CHECKING:
    from argparse import Namespace


@contextmanager
def viztracer_profile(
    path: Union[Callable[[], str], str],
    max_stack_depth: int = -1,
):
    try:
        import viztracer  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, viztracer is not installed")
        yield
        return

    tracer = viztracer.VizTracer(max_stack_depth=max_stack_depth)

    tracer.start()
    yield
    tracer.stop()

    tracer.save(path() if callable(path) else path)


@contextmanager
def yappi_profile(
    path: Union[Callable[[], str], str] = None, wall_clock: bool = True
):
    try:
        import yappi  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, yappi is not installed")
        yield
        return

    yappi.set_clock_type("wall" if wall_clock else "cpu")

    yappi.start()
    yield
    yappi.stop()

    # pylint:disable=no-member
    if path:
        stats = yappi.get_func_stats()
        fpath = path() if callable(path) else path
        stats.save(fpath, "callgrind")
    else:
        yappi.get_func_stats().print_all()
        yappi.get_thread_stats().print_all()

    yappi.clear_stats()


@contextmanager
def instrument(html_output=False):
    """Run a statistical profiler"""
    try:
        from pyinstrument import Profiler  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, pyinstrument is not installed")
        yield
        return

    profiler = Profiler()

    profiler.start()
    yield
    profiler.stop()

    if html_output:
        profiler.open_in_browser()
        return
    print(profiler.output_text(unicode=True, color=True))


@contextmanager
def profile(dump_path: str = None):
    """Run a cprofile"""
    import cProfile

    prof = cProfile.Profile()
    prof.enable()

    yield

    prof.disable()
    if not dump_path:
        prof.print_stats(sort="cumtime")
        return
    prof.dump_stats(dump_path)


@contextmanager
def debug():
    try:
        yield
    except Exception:  # pylint: disable=broad-except
        try:
            import ipdb as pdb  # noqa: T100, pylint: disable=import-error
        except ImportError:
            import pdb  # noqa: T100
        pdb.post_mortem()

        raise  # prevent from jumping ahead


@contextmanager
def debugtools(args: "Namespace" = None, **kwargs):
    kw = vars(args) if args else {}
    kw.update(kwargs)

    with ExitStack() as stack:
        if kw.get("pdb"):
            stack.enter_context(debug())
        if kw.get("cprofile") or kw.get("cprofile_dump"):
            stack.enter_context(profile(kw.get("cprofile_dump")))
        if kw.get("instrument") or kw.get("instrument_open"):
            stack.enter_context(instrument(kw.get("instrument_open", False)))
        if kw.get("yappi"):
            output = "callgrind.dvc-{0:%Y%m%d}_{0:%H%M%S}.out"
            stack.enter_context(
                yappi_profile(path=lambda: output.format(datetime.now()))
            )
        if kw.get("viztracer") or kw.get("viztracer_depth"):
            output = "viztracer.dvc-{0:%Y%m%d}_{0:%H%M%S}.json"
            stack.enter_context(
                viztracer_profile(
                    path=lambda: output.format(datetime.now()),
                    max_stack_depth=kw.get("viztracer_depth") or -1,
                )
            )
        yield


def add_debugging_flags(parser):
    from argparse import SUPPRESS

    parser.add_argument(
        "--cprofile", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--yappi", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--viztracer", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument("--viztracer-depth", type=int, help=SUPPRESS)
    parser.add_argument("--cprofile-dump", help=SUPPRESS)
    parser.add_argument(
        "--pdb", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--instrument", action="store_true", default=False, help=SUPPRESS
    )
    parser.add_argument(
        "--instrument-open", action="store_true", default=False, help=SUPPRESS
    )
