import argparse
from contextlib import ExitStack, contextmanager


@contextmanager
def instrument(html_output):
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
def profile(dump):
    """Run a cprofile"""
    import cProfile

    prof = cProfile.Profile()
    prof.enable()

    yield

    prof.disable()
    if not dump:
        prof.print_stats(sort="cumtime")
        return
    prof.dump_stats(dump)


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


@contextmanager
def debugtools(args):
    with ExitStack() as stack:
        if args.pdb:
            stack.enter_context(debug())
        if args.cprofile:
            stack.enter_context(profile(args.cprofile_dump))
        if args.instrument:
            stack.enter_context(instrument(args.instrument_open))
        yield


def add_debugging_flags(parser):
    parser.add_argument(
        "--cprofile",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--cprofile-dump", help=argparse.SUPPRESS)
    parser.add_argument(
        "--pdb", action="store_true", default=False, help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--instrument",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--instrument-open",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS,
    )
