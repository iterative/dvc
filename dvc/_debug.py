from contextlib import ExitStack, contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional, Union

if TYPE_CHECKING:
    from argparse import Namespace
    from types import FrameType


@contextmanager
def viztracer_profile(
    path: Union[Callable[[], str], str],
    depth: int = -1,
    log_async: bool = False,
):
    try:
        import viztracer  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, viztracer is not installed")  # noqa: T201
        yield
        return

    tracer = viztracer.VizTracer(max_stack_depth=depth, log_async=log_async)

    tracer.start()
    yield
    tracer.stop()

    tracer.save(path() if callable(path) else path)


@contextmanager
def yappi_profile(
    path: Optional[Union[Callable[[], str], str]] = None,
    wall_clock: Optional[bool] = True,
    separate_threads: Optional[bool] = False,
):
    try:
        import yappi  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, yappi is not installed")  # noqa: T201
        yield
        return

    yappi.set_clock_type("wall" if wall_clock else "cpu")

    yappi.start()
    yield
    yappi.stop()

    threads = yappi.get_thread_stats()
    stats = {}
    if separate_threads:
        for thread in threads:
            ctx_id = thread.id
            stats[ctx_id] = yappi.get_func_stats(ctx_id=ctx_id)
    else:
        stats[None] = yappi.get_func_stats()

    fpath = path() if callable(path) else path
    for ctx_id, st in stats.items():
        if fpath:
            out = f"{fpath}-{ctx_id}" if ctx_id is not None else fpath
            st.save(out, type="callgrind")
        else:
            if ctx_id is not None:
                print(f"\nThread {ctx_id}")  # noqa: T201
            st.print_all()  # pylint:disable=no-member
            if ctx_id is None:
                threads.print_all()  # pylint:disable=no-member

    yappi.clear_stats()


@contextmanager
def instrument(html_output=False):
    """Run a statistical profiler"""
    try:
        from pyinstrument import Profiler  # pylint: disable=import-error
    except ImportError:
        print("Failed to run profiler, pyinstrument is not installed")  # noqa: T201
        yield
        return

    profiler = Profiler()

    profiler.start()
    yield
    profiler.stop()

    if html_output:
        profiler.open_in_browser()
        return
    print(profiler.output_text(unicode=True, color=True))  # noqa: T201


@contextmanager
def profile(dump_path: Optional[str] = None):
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
            import pdb  # type: ignore[no-redef]  # noqa: T100
        pdb.post_mortem()

        raise  # prevent from jumping ahead


def _sigshow(_, frame: Optional["FrameType"]) -> None:
    import sys
    from shutil import get_terminal_size
    from traceback import format_stack

    lines = "\u2015" * get_terminal_size().columns
    stack = format_stack(frame)
    print(lines, "\n", *stack, lines, sep="", file=sys.stderr)  # noqa: T201


@contextmanager
def show_stack():
    r"""Show stack trace on SIGQUIT (Ctrl-\) or SIGINFO (Ctrl-T on macOS)."""
    import signal
    import sys

    if sys.platform != "win32":
        signal.signal(signal.SIGQUIT, _sigshow)

    try:
        signal.signal(signal.SIGINFO, _sigshow)  # only available on macOS
    except AttributeError:
        pass
    yield


def _get_path_func(tool: str, ext: str):
    fmt = f"{tool}.dvc-{{now:%Y%m%d}}_{{now:%H%M%S}}.{ext}"

    def func(now: Optional["datetime"] = None) -> str:
        return fmt.format(now=now or datetime.now())

    return func


@contextmanager
def debugtools(args: Optional["Namespace"] = None, **kwargs):
    kw = vars(args) if args else {}
    kw.update(kwargs)

    with ExitStack() as stack:
        if kw.get("pdb"):
            stack.enter_context(debug())
        if kw.get("cprofile") or kw.get("cprofile_dump"):
            stack.enter_context(profile(kw.get("cprofile_dump")))
        if kw.get("instrument") or kw.get("instrument_open"):
            stack.enter_context(instrument(kw.get("instrument_open", False)))
        if kw.get("show_stack", False):
            stack.enter_context(show_stack())
        if kw.get("yappi"):
            path_func = _get_path_func("callgrind", "out")
            stack.enter_context(
                yappi_profile(
                    path=path_func,
                    separate_threads=kw.get("yappi_separate_threads"),
                )
            )
        if (
            kw.get("viztracer")
            or kw.get("viztracer_depth")
            or kw.get("viztracer_async")
        ):
            path_func = _get_path_func("viztracer", "json")
            depth = kw.get("viztracer_depth") or -1
            log_async = kw.get("viztracer_async") or False
            prof = viztracer_profile(path=path_func, depth=depth, log_async=log_async)
            stack.enter_context(prof)
        yield


def add_debugging_flags(parser):
    from argparse import SUPPRESS

    # For detailed info see:
    # https://github.com/iterative/dvc/wiki/Debugging,-Profiling-and-Benchmarking-DVC
    args, _ = parser.parse_known_args()
    verbose = args.verbose

    def debug_help(msg):
        if verbose:
            return msg
        return SUPPRESS

    parser = parser.add_argument_group("debug options")

    parser.add_argument(
        "--cprofile",
        action="store_true",
        default=False,
        help=debug_help("Generate cprofile data for tools like snakeviz / tuna"),
    )
    parser.add_argument(
        "--cprofile-dump", help=debug_help("Location to dump cprofile file")
    )
    parser.add_argument(
        "--yappi",
        action="store_true",
        default=False,
        help=debug_help(
            "Generate a callgrind file for use with tools like "
            "kcachegrind / qcachegrind"
        ),
    )
    parser.add_argument(
        "--yappi-separate-threads",
        action="store_true",
        default=False,
        help=debug_help("Generate one callgrind file per thread"),
    )
    parser.add_argument(
        "--viztracer",
        action="store_true",
        default=False,
        help=debug_help("Generate a viztracer file for use with vizviewer"),
    )
    parser.add_argument(
        "--viztracer-depth",
        type=int,
        help=debug_help("Set viztracer maximum stack depth"),
    )
    parser.add_argument(
        "--viztracer-async",
        action="store_true",
        default=False,
        help=debug_help("Treat async tasks as threads"),
    )
    parser.add_argument(
        "--pdb",
        action="store_true",
        default=False,
        help=debug_help("Drop into the pdb/ipdb debugger on any exception"),
    )
    parser.add_argument(
        "--instrument",
        action="store_true",
        default=False,
        help=debug_help("Use pyinstrument CLI profiler"),
    )
    parser.add_argument(
        "--instrument-open",
        action="store_true",
        default=False,
        help=debug_help("Use pyinstrument web profiler"),
    )
    parser.add_argument(
        "--show-stack",
        "--ss",
        action="store_true",
        default=False,
        help=debug_help(
            r"Use Ctrl+T on macOS or Ctrl+\ on Linux to print the stack "
            "frame currently executing. Unavailable on Windows."
        ),
    )
