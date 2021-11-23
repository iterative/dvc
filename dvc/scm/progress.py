from typing import NamedTuple, Optional, Union

from funcy import compose


def code2desc(op_code):
    from git import RootUpdateProgress as OP

    ops = {
        OP.COUNTING: "Counting",
        OP.COMPRESSING: "Compressing",
        OP.WRITING: "Writing",
        OP.RECEIVING: "Receiving",
        OP.RESOLVING: "Resolving",
        OP.FINDING_SOURCES: "Finding sources",
        OP.CHECKING_OUT: "Checking out",
        OP.CLONE: "Cloning",
        OP.FETCH: "Fetching",
        OP.UPDWKTREE: "Updating working tree",
        OP.REMOVE: "Removing",
        OP.PATHCHANGE: "Changing path",
        OP.URLCHANGE: "Changing URL",
        OP.BRANCHCHANGE: "Changing branch",
    }
    return ops.get(op_code & OP.OP_MASK, "")


class GitProgressEvent(NamedTuple):
    phase: str = ""
    completed: Optional[int] = None
    total: Optional[int] = None
    message: str = ""

    @classmethod
    def parsed_from_gitpython(
        cls,
        op_code,
        cur_count,
        max_count=None,
        message="",  # pylint: disable=redefined-outer-name
    ):
        return cls(code2desc(op_code), cur_count, max_count, message)


class GitProgressReporter:
    def __init__(self, fn) -> None:
        from git.util import CallableRemoteProgress

        self._reporter = CallableRemoteProgress(self.wrap_fn(fn))

    def __call__(self, msg: Union[str, bytes]) -> None:
        self._reporter._parse_progress_line(
            msg.decode("ascii").strip() if isinstance(msg, bytes) else msg
        )

    @staticmethod
    def wrap_fn(fn):
        return compose(fn, GitProgressEvent.parsed_from_gitpython)


if __name__ == "__main__":
    message = """
Cloning into 'dvcyaml-schema'...
remote: Enumerating objects: 76, done.
remote: Counting objects: 100% (76/76), done.
remote: Compressing objects: 100% (56/56), done.
remote: Total 76 (delta 30), reused 38 (delta 9), pack-reused 0
Receiving objects: 100% (76/76), 24.83 KiB | 89.00 KiB/s, done.
Resolving deltas: 100% (30/30), done."""

    reporter = GitProgressReporter(print)
    for line in message.splitlines():
        reporter(line)
