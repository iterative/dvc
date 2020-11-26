import logging
import typing
from typing import Iterable, List

from funcy.strings import re_tester

from dvc.dvcfile import PIPELINE_FILE, Dvcfile
from dvc.utils import parse_target

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.stage.loader import StageLoader


class StageLoad:
    def __init__(self, repo: "Repo") -> None:
        self.repo = repo

    def from_target(
        self,
        target: str,
        accept_group: bool = False,
        filter_regex: bool = False,
    ) -> List["Stage"]:
        """
        Returns a list of stage from the provided target.
        (see load method below for further details)
        """
        path, name = parse_target(target, isa_regex=filter_regex)
        return self.load_all(
            path=path,
            name=name,
            accept_group=accept_group,
            filter_regex=filter_regex,
        )

    def get_target(self, target: str) -> "Stage":
        """
        Returns a stage from the provided target.
        (see load_one method for further details)
        """
        path, name = parse_target(target)
        return self.load_one(path=path, name=name)

    @staticmethod
    def _get_filepath(path: str = None, name: str = None) -> str:
        if path:
            return path

        path = PIPELINE_FILE
        logger.debug("Assuming '%s' to be a stage inside '%s'", name, path)
        return path

    @staticmethod
    def _get_group_keys(stages: "StageLoader", group: str) -> Iterable[str]:
        from dvc.parsing import JOIN

        for key in stages:
            assert isinstance(key, str)
            if key.startswith(f"{group}{JOIN}"):
                yield key

    def _get_keys(
        self,
        stages: "StageLoader",
        name: str = None,
        accept_group: bool = False,
        filter_regex: bool = False,
    ) -> Iterable[str]:

        assert not (accept_group and filter_regex)

        if not name:
            return stages.keys()

        if accept_group and stages.is_foreach_generated(name):
            return self._get_group_keys(stages, name)
        elif filter_regex:
            filter_fn = re_tester(name)
            return filter(filter_fn, stages.keys())
        return [name]

    def load_all(
        self,
        path: str = None,
        name: str = None,
        accept_group: bool = False,
        filter_regex: bool = False,
    ) -> List["Stage"]:
        """Load a list of stages from a file.

        Args:
            path: if not provided, default `dvc.yaml` is assumed.
            name: required for `dvc.yaml` files, ignored for `.dvc` files.
            accept_group: if true, all of the the stages generated from `name`
                foreach are returned.
            filter_regex: if true, `name` is considered as regex, which is
                used to filter list of stages from the given `path`.
        """
        from dvc.stage.loader import SingleStageLoader, StageLoader

        path = self._get_filepath(path, name)
        dvcfile = Dvcfile(self.repo, path)
        # `dvcfile.stages` is not cached
        stages = dvcfile.stages  # type: ignore

        if isinstance(stages, SingleStageLoader):
            return [stages[name]]

        assert isinstance(stages, StageLoader)
        keys = self._get_keys(stages, name, accept_group, filter_regex)
        return [stages[key] for key in keys]

    def load_one(self, path: str = None, name: str = None) -> "Stage":
        """Load a single stage from a file.

        Args:
            path: if not provided, default `dvc.yaml` is assumed.
            name: required for `dvc.yaml` files, ignored for `.dvc` files.
        """
        path = self._get_filepath(path, name)
        dvcfile = Dvcfile(self.repo, path)

        stages = dvcfile.stages  # type: ignore

        return stages[name]

    def load_file(self, path: str = None) -> List["Stage"]:
        """Load all of the stages from a file."""
        return self.load_all(path)

    def load_glob(self, path: str, expr: str = None):
        """Load stages from `path`, filtered with `expr` provided."""
        return self.load_all(path, expr, filter_regex=True)
