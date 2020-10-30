import logging
import os
from contextlib import contextmanager
from functools import wraps

from funcy import cached_property, cat, first
from git import InvalidGitRepositoryError

from dvc.config import Config
from dvc.dvcfile import PIPELINE_FILE, Dvcfile, is_valid_filename
from dvc.exceptions import FileMissingError
from dvc.exceptions import IsADirectoryError as DvcIsADirectoryError
from dvc.exceptions import (
    NoOutputOrStageError,
    NotDvcRepoError,
    OutputNotFoundError,
)
from dvc.path_info import PathInfo
from dvc.scm import Base
from dvc.scm.base import SCMError
from dvc.tree.repo import RepoTree
from dvc.utils.fs import path_isin

from ..stage.exceptions import StageFileDoesNotExistError, StageNotFound
from ..utils import parse_target
from .graph import check_acyclic, get_pipeline, get_pipelines

logger = logging.getLogger(__name__)


@contextmanager
def lock_repo(repo):
    # pylint: disable=protected-access
    depth = getattr(repo, "_lock_depth", 0)
    repo._lock_depth = depth + 1

    try:
        if depth > 0:
            yield
        else:
            with repo.lock, repo.state:
                repo._reset()
                yield
                # Graph cache is no longer valid after we release the repo.lock
                repo._reset()
    finally:
        repo._lock_depth = depth


def locked(f):
    @wraps(f)
    def wrapper(repo, *args, **kwargs):
        with lock_repo(repo):
            return f(repo, *args, **kwargs)

    return wrapper


class Repo:
    DVC_DIR = ".dvc"

    from dvc.repo.add import add
    from dvc.repo.brancher import brancher
    from dvc.repo.checkout import checkout
    from dvc.repo.commit import commit
    from dvc.repo.destroy import destroy
    from dvc.repo.diff import diff
    from dvc.repo.fetch import fetch
    from dvc.repo.freeze import freeze, unfreeze
    from dvc.repo.gc import gc
    from dvc.repo.get import get
    from dvc.repo.get_url import get_url
    from dvc.repo.imp import imp
    from dvc.repo.imp_url import imp_url
    from dvc.repo.install import install
    from dvc.repo.ls import ls
    from dvc.repo.move import move
    from dvc.repo.pull import pull
    from dvc.repo.push import push
    from dvc.repo.remove import remove
    from dvc.repo.reproduce import reproduce
    from dvc.repo.run import run
    from dvc.repo.status import status
    from dvc.repo.update import update

    def _get_repo_dirs(
        self,
        root_dir: str = None,
        scm: Base = None,
        rev: str = None,
        uninitialized: bool = False,
    ):
        assert bool(scm) == bool(rev)

        from dvc.scm import SCM
        from dvc.utils.fs import makedirs

        try:
            tree = scm.get_tree(rev) if rev else None
            root_dir = self.find_root(root_dir, tree)
            dvc_dir = os.path.join(root_dir, self.DVC_DIR)
            tmp_dir = os.path.join(dvc_dir, "tmp")
            makedirs(tmp_dir, exist_ok=True)

        except NotDvcRepoError:
            if not uninitialized:
                raise
            try:
                root_dir = SCM(root_dir or os.curdir).root_dir
            except (SCMError, InvalidGitRepositoryError):
                root_dir = SCM(os.curdir, no_scm=True).root_dir

            dvc_dir = None
            tmp_dir = None
        return root_dir, dvc_dir, tmp_dir

    def __init__(
        self,
        root_dir=None,
        scm=None,
        rev=None,
        subrepos=False,
        uninitialized=False,
    ):
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.lock import LockNoop, make_lock
        from dvc.repo.experiments import Experiments
        from dvc.repo.metrics import Metrics
        from dvc.repo.params import Params
        from dvc.repo.plots import Plots
        from dvc.stage.cache import StageCache
        from dvc.state import State, StateNoop
        from dvc.tree.local import LocalTree

        self.root_dir, self.dvc_dir, self.tmp_dir = self._get_repo_dirs(
            root_dir=root_dir, scm=scm, rev=rev, uninitialized=uninitialized
        )

        tree_kwargs = {"use_dvcignore": True, "dvcignore_root": self.root_dir}
        if scm:
            self.tree = scm.get_tree(rev, **tree_kwargs)
        else:
            self.tree = LocalTree(self, {"url": self.root_dir}, **tree_kwargs)

        self.config = Config(self.dvc_dir, tree=self.tree)
        self._scm = scm

        # used by RepoTree to determine if it should traverse subrepos
        self.subrepos = subrepos

        self.cache = Cache(self)
        self.cloud = DataCloud(self)

        if scm or not self.dvc_dir:
            self.lock = LockNoop()
            self.state = StateNoop()
        else:
            self.lock = make_lock(
                os.path.join(self.tmp_dir, "lock"),
                tmp_dir=self.tmp_dir,
                hardlink_lock=self.config["core"].get("hardlink_lock", False),
                friendly=True,
            )

            # NOTE: storing state and link_state in the repository itself to
            # avoid any possible state corruption in 'shared cache dir'
            # scenario.
            self.state = State(self)
            self.stage_cache = StageCache(self)

            try:
                self.experiments = Experiments(self)
            except NotImplementedError:
                self.experiments = None

            self._ignore()

        self.metrics = Metrics(self)
        self.plots = Plots(self)
        self.params = Params(self)

    @cached_property
    def scm(self):
        from dvc.scm import SCM

        no_scm = self.config["core"].get("no_scm", False)
        return self._scm if self._scm else SCM(self.root_dir, no_scm=no_scm)

    @property
    def tree(self):
        return self._tree

    @tree.setter
    def tree(self, tree):
        self._tree = tree
        # Our graph cache is no longer valid, as it was based on the previous
        # tree.
        self._reset()

    def __repr__(self):
        return f"{self.__class__.__name__}: '{self.root_dir}'"

    @classmethod
    def find_root(cls, root=None, tree=None):
        root_dir = os.path.realpath(root or os.curdir)

        if tree:
            if tree.isdir(os.path.join(root_dir, cls.DVC_DIR)):
                return root_dir
            raise NotDvcRepoError(f"'{root}' does not contain DVC directory")

        if not os.path.isdir(root_dir):
            raise NotDvcRepoError(f"directory '{root}' does not exist")

        while True:
            dvc_dir = os.path.join(root_dir, cls.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root_dir
            if os.path.ismount(root_dir):
                break
            root_dir = os.path.dirname(root_dir)

        message = (
            "you are not inside of a DVC repository "
            "(checked up to mount point '{}')"
        ).format(root_dir)
        raise NotDvcRepoError(message)

    @classmethod
    def find_dvc_dir(cls, root=None):
        root_dir = cls.find_root(root)
        return os.path.join(root_dir, cls.DVC_DIR)

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False, subdir=False):
        from dvc.repo.init import init

        init(root_dir=root_dir, no_scm=no_scm, force=force, subdir=subdir)
        return Repo(root_dir)

    def unprotect(self, target):
        return self.cache.local.tree.unprotect(PathInfo(target))

    def _ignore(self):
        flist = [
            self.config.files["local"],
            self.tmp_dir,
        ]
        if self.experiments:
            flist.append(self.experiments.exp_dir)

        if path_isin(self.cache.local.cache_dir, self.root_dir):
            flist += [self.cache.local.cache_dir]

        self.scm.ignore_list(flist)

    def get_stage(self, path=None, name=None):
        if not path:
            path = PIPELINE_FILE
            logger.debug("Assuming '%s' to be a stage inside '%s'", name, path)

        dvcfile = Dvcfile(self, path)
        return dvcfile.stages[name]

    def get_stages(self, path=None, name=None):
        if not path:
            path = PIPELINE_FILE
            logger.debug("Assuming '%s' to be a stage inside '%s'", name, path)

        if name:
            return [self.get_stage(path, name)]

        dvcfile = Dvcfile(self, path)
        return list(dvcfile.stages.values())

    def check_modified_graph(self, new_stages):
        """Generate graph including the new stage to check for errors"""
        # Building graph might be costly for the ones with many DVC-files,
        # so we provide this undocumented hack to skip it. See [1] for
        # more details. The hack can be used as:
        #
        #     repo = Repo(...)
        #     repo._skip_graph_checks = True
        #     repo.add(...)
        #
        # A user should care about not duplicating outs and not adding cycles,
        # otherwise DVC might have an undefined behaviour.
        #
        # [1] https://github.com/iterative/dvc/issues/2671
        if not getattr(self, "_skip_graph_checks", False):
            self._collect_graph(self.stages + new_stages)

    def _collect_inside(self, path, graph):
        import networkx as nx

        stages = nx.dfs_postorder_nodes(graph)
        return [stage for stage in stages if path_isin(stage.path, path)]

    def collect(
        self, target=None, with_deps=False, recursive=False, graph=None
    ):
        if not target:
            return list(graph) if graph else self.stages

        if recursive and os.path.isdir(target):
            return self._collect_inside(
                os.path.abspath(target), graph or self.graph
            )

        path, name = parse_target(target)
        stages = self.get_stages(path, name)
        if not with_deps:
            return stages

        res = set()
        for stage in stages:
            res.update(self._collect_pipeline(stage, graph=graph))
        return res

    def _collect_pipeline(self, stage, graph=None):
        import networkx as nx

        pipeline = get_pipeline(get_pipelines(graph or self.graph), stage)
        return nx.dfs_postorder_nodes(pipeline, stage)

    def _collect_from_default_dvcfile(self, target):
        dvcfile = Dvcfile(self, PIPELINE_FILE)
        if dvcfile.exists():
            return dvcfile.stages.get(target)

    def collect_granular(
        self, target=None, with_deps=False, recursive=False, graph=None
    ):
        """
        Priority is in the order of following in case of ambiguity:
            - .dvc file or .yaml file
            - dir if recursive and directory exists
            - stage_name
            - output file
        """
        if not target:
            return [(stage, None) for stage in self.stages]

        file, name = parse_target(target)
        stages = []

        # Optimization: do not collect the graph for a specific target
        if not file:
            # parsing is ambiguous when it does not have a colon
            # or if it's not a dvcfile, as it can be a stage name
            # in `dvc.yaml` or, an output in a stage.
            logger.debug(
                "Checking if stage '%s' is in '%s'", target, PIPELINE_FILE
            )
            if not (recursive and os.path.isdir(target)):
                stage = self._collect_from_default_dvcfile(target)
                if stage:
                    stages = (
                        self._collect_pipeline(stage) if with_deps else [stage]
                    )
        elif not with_deps and is_valid_filename(file):
            stages = self.get_stages(file, name)

        if not stages:
            if not (recursive and os.path.isdir(target)):
                try:
                    (out,) = self.find_outs_by_path(target, strict=False)
                    filter_info = PathInfo(os.path.abspath(target))
                    return [(out.stage, filter_info)]
                except OutputNotFoundError:
                    pass

            try:
                stages = self.collect(target, with_deps, recursive, graph)
            except StageFileDoesNotExistError as exc:
                # collect() might try to use `target` as a stage name
                # and throw error that dvc.yaml does not exist, whereas it
                # should say that both stage name and file does not exist.
                if file and is_valid_filename(file):
                    raise
                raise NoOutputOrStageError(target, exc.file) from exc
            except StageNotFound as exc:
                raise NoOutputOrStageError(target, exc.file) from exc

        return [(stage, None) for stage in stages]

    def used_cache(
        self,
        targets=None,
        all_branches=False,
        with_deps=False,
        all_tags=False,
        all_commits=False,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
        used_run_cache=None,
    ):
        """Get the stages related to the given target and collect
        the `info` of its outputs.

        This is useful to know what files from the cache are _in use_
        (namely, a file described as an output on a stage).

        The scope is, by default, the working directory, but you can use
        `all_branches`/`all_tags`/`all_commits` to expand the scope.

        Returns:
            A dictionary with Schemes (representing output's location) mapped
            to items containing the output's `dumpd` names and the output's
            children (if the given output is a directory).
        """
        from dvc.cache import NamedCache

        cache = NamedCache()

        for branch in self.brancher(
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
        ):
            targets = targets or [None]

            pairs = cat(
                self.collect_granular(
                    target, recursive=recursive, with_deps=with_deps
                )
                for target in targets
            )

            suffix = f"({branch})" if branch else ""
            for stage, filter_info in pairs:
                used_cache = stage.get_used_cache(
                    remote=remote,
                    force=force,
                    jobs=jobs,
                    filter_info=filter_info,
                )
                cache.update(used_cache, suffix=suffix)

        if used_run_cache:
            used_cache = self.stage_cache.get_used_cache(
                used_run_cache, remote=remote, force=force, jobs=jobs,
            )
            cache.update(used_cache)

        return cache

    def _collect_graph(self, stages):
        """Generate a graph by using the given stages on the given directory

        The nodes of the graph are the stage's path relative to the root.

        Edges are created when the output of one stage is used as a
        dependency in other stage.

        The direction of the edges goes from the stage to its dependency:

        For example, running the following:

            $ dvc run -o A "echo A > A"
            $ dvc run -d A -o B "echo B > B"
            $ dvc run -d B -o C "echo C > C"

        Will create the following graph:

               ancestors <--
                           |
                C.dvc -> B.dvc -> A.dvc
                |          |
                |          --> descendants
                |
                ------- pipeline ------>
                           |
                           v
              (weakly connected components)

        Args:
            stages (list): used to build a graph, if None given, collect stages
                in the repository.

        Raises:
            OutputDuplicationError: two outputs with the same path
            StagePathAsOutputError: stage inside an output directory
            OverlappingOutputPathsError: output inside output directory
            CyclicGraphError: resulting graph has cycles
        """
        import networkx as nx
        from pygtrie import Trie

        from dvc.exceptions import (
            OutputDuplicationError,
            OverlappingOutputPathsError,
            StagePathAsOutputError,
        )

        G = nx.DiGraph()
        stages = stages or self.stages
        outs = Trie()  # Use trie to efficiently find overlapping outs and deps

        for stage in filter(bool, stages):  # bug? not using it later
            for out in stage.outs:
                out_key = out.path_info.parts

                # Check for dup outs
                if out_key in outs:
                    dup_stages = [stage, outs[out_key].stage]
                    raise OutputDuplicationError(str(out), dup_stages)

                # Check for overlapping outs
                if outs.has_subtrie(out_key):
                    parent = out
                    overlapping = first(outs.values(prefix=out_key))
                else:
                    parent = outs.shortest_prefix(out_key).value
                    overlapping = out
                if parent and overlapping:
                    msg = (
                        "Paths for outs:\n'{}'('{}')\n'{}'('{}')\n"
                        "overlap. To avoid unpredictable behaviour, "
                        "rerun command with non overlapping outs paths."
                    ).format(
                        str(parent),
                        parent.stage.addressing,
                        str(overlapping),
                        overlapping.stage.addressing,
                    )
                    raise OverlappingOutputPathsError(parent, overlapping, msg)

                outs[out_key] = out

        for stage in stages:
            out = outs.shortest_prefix(PathInfo(stage.path).parts).value
            if out:
                raise StagePathAsOutputError(stage, str(out))

        # Building graph
        G.add_nodes_from(stages)
        for stage in stages:
            for dep in stage.deps:
                if dep.path_info is None:
                    continue

                dep_key = dep.path_info.parts
                overlapping = [n.value for n in outs.prefixes(dep_key)]
                if outs.has_subtrie(dep_key):
                    overlapping.extend(outs.values(prefix=dep_key))

                G.add_edges_from((stage, out.stage) for out in overlapping)
        check_acyclic(G)

        return G

    @cached_property
    def graph(self):
        return self._collect_graph(self.stages)

    @cached_property
    def pipelines(self):
        return get_pipelines(self.graph)

    @cached_property
    def stages(self):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large repos, this could be an expensive
              operation. Consider using some memoization.
        """
        return self._collect_stages()

    def _collect_stages(self):
        stages = []
        outs = set()

        for root, dirs, files in self.tree.walk(self.root_dir):
            for file_name in filter(is_valid_filename, files):
                new_stages = self.get_stages(os.path.join(root, file_name))
                stages.extend(new_stages)
                outs.update(
                    out.fspath
                    for stage in new_stages
                    for out in stage.outs
                    if out.scheme == "local"
                )
            dirs[:] = [d for d in dirs if os.path.join(root, d) not in outs]
        return stages

    def find_outs_by_path(self, path, outs=None, recursive=False, strict=True):
        if not outs:
            outs = [out for stage in self.stages for out in stage.outs]

        abs_path = os.path.abspath(path)
        path_info = PathInfo(abs_path)
        match = path_info.__eq__ if strict else path_info.isin_or_eq

        def func(out):
            if out.scheme == "local" and match(out.path_info):
                return True

            if recursive and out.path_info.isin(path_info):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path, self)

        return matched

    def find_out_by_relpath(self, relpath):
        path = os.path.join(self.root_dir, relpath)
        (out,) = self.find_outs_by_path(path)
        return out

    def is_dvc_internal(self, path):
        path_parts = os.path.normpath(path).split(os.path.sep)
        return self.DVC_DIR in path_parts

    @cached_property
    def repo_tree(self):
        return RepoTree(self, subrepos=self.subrepos, fetch=True)

    @contextmanager
    def open_by_relpath(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file descriptor"""

        tree = RepoTree(self, stream=True, subrepos=True)
        path = PathInfo(self.root_dir) / path
        try:
            with self.state:
                with tree.open(
                    path, mode=mode, encoding=encoding, remote=remote,
                ) as fobj:
                    yield fobj
        except FileNotFoundError as exc:
            raise FileMissingError(path) from exc
        except IsADirectoryError as exc:
            raise DvcIsADirectoryError(f"'{path}' is a directory") from exc

    def close(self):
        self.scm.close()

    def _reset(self):
        self.__dict__.pop("graph", None)
        self.__dict__.pop("stages", None)
        self.__dict__.pop("pipelines", None)
