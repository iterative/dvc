from dataclasses import dataclass, field
from typing import List

from dvc.output import BaseOutput
from dvc.path_info import PathInfo


@dataclass
class Metadata:
    """
    Container for storing metadata for a given path, similar to `stat_result`
    """

    # required field
    path_info: PathInfo

    # computed fields
    is_output: bool = field(init=False, default=False)  # is it an output?
    # is the path part of an output?
    part_of_output: bool = field(init=False, default=False)
    # does the path contain outputs?
    contains_outputs: bool = field(init=False, default=False)
    # does the path have any dvc outputs?
    is_dvc: bool = field(init=False)  # equivalent to len(outs) >= 1

    # optional fields
    outs: List[BaseOutput] = field(default_factory=list)
    # list of outputs inside that path
    # if `is_output` is True, there will be only one output inside it
    is_exec: bool = False  # executable?
    isdir: bool = False  # is it a directory?

    def __post_init__(self):
        self.is_dvc = bool(self.outs)

        if not self.is_dvc:
            return

        # it can contain multiple outputs
        if len(self.outs) > 1:
            self.contains_outputs = True
        else:
            out = self.outs[0]
            # or, the path itself could be an output
            self.is_output = self.path_info == out.path_info

            # or, a directory must have an output somewhere deep inside it
            if not self.is_output:
                self.contains_outputs = out.path_info.isin(self.path_info)

        # or, the path could be a part of an output, i.e. inside of an output
        self.part_of_output = not self.is_output and not self.contains_outputs
        # if it contains outputs, it must be a dir
        self.isdir = self.contains_outputs

    @property
    def isfile(self):
        return not self.isdir

    def __str__(self):
        return f"Metadata: {self.path_info}"
