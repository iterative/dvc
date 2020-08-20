from dataclasses import dataclass, field
from typing import List

from dvc.output import BaseOutput
from dvc.path_info import PathInfo


@dataclass
class Metadata:
    # required fields
    path_info: PathInfo
    # NOTE: due to how we retrieve `outs`, we won't be able to fetch outputs
    #  from sub-repos at all.
    outs: List[BaseOutput]  # list of  outputs inside that path
    # if `is_output` is True, there will be only one outputs inside it

    # computed fields
    is_output: bool = field(init=False, default=False)  # is it an output?
    # is the path part of an output?
    part_of_output: bool = field(init=False, default=False)
    # does the path contain outputs?
    contains_outputs: bool = field(init=False, default=False)
    # does the path have any dvc outputs?
    is_dvc: bool = field(init=False)  # equivalent to len(outs) >= 1

    # optional fields
    is_exec: bool = False  # executable?
    isdir: bool = False  # is it a directory?

    def __post_init__(self):
        self.is_dvc = bool(self.outs)

        if not self.is_dvc:
            return

        if len(self.outs) > 1:
            self.contains_outputs = True
        else:
            out = self.outs[0]
            # otherwise the path itself could be an output
            self.is_output = self.path_info == out.path_info
            # or a directory must have an output somewhere deep inside that dir
            if self.is_output:
                self.contains_outputs = out.path_info.isin(self.path_info)
            # or, the path could be a part of an output
            self.part_of_output = (
                not self.is_output and not self.contains_outputs
            )

        # if it contains outputs, it must be a dir
        self.isdir = self.contains_outputs

    @property
    def isfile(self):
        return not self.isdir
