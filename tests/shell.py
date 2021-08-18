import subprocess
from typing import Sequence, Union

from dvc.types import StrPath

CMD = Union[bytes, str, Sequence[StrPath]]


class Command:
    def __init__(self, *args: CMD) -> None:
        self._args = list(args)

    def args(self, *args: CMD) -> "Command":
        return Command(*self._args, *args)

    def run(self, *args: CMD) -> "subprocess.CompletedProcess":
        return subprocess.run(  # pylint: disable=subprocess-run-check
            [*self._args, *args], capture_output=True, text=True
        )

    def __call__(self, *args: CMD) -> "subprocess.CompletedProcess":
        return self.run(*args)
