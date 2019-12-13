from __future__ import unicode_literals

import logging
import os
from contextlib import contextmanager
from functools import wraps
from itertools import chain

from funcy import cached_property

from .graph import check_acyclic
from .graph import get_pipeline
from .graph import get_pipelines
from .graph import get_stages
from dvc.config import Config
from dvc.exceptions import FileMissingError
from dvc.exceptions import NotDvcRepoError
from dvc.exceptions import OutputNotFoundError
from dvc.ignore import DvcIgnoreFilter
from dvc.path_info import PathInfo
from dvc.remote.base import RemoteActionNotImplemented
from dvc.utils import relpath
from dvc.utils.fs import path_isin
from dvc.utils.compat import FileNotFoundError
from dvc.utils.compat import fspath_py35
from dvc.utils.compat import open as _open


logger = logging.getLogger(__name__)


def locked(f):
    @wraps(f)
    def wrapper(repo, *args, **kwargs):
        with repo.lock, repo.state:
            ret = f(repo, *args, **kwargs)
            # Our graph cache is no longer valid after we release the repo.lock
            repo._reset()
            return ret

    return wrapper


class Repo(object):
    DVC_DIR = ".dvc"

    from dvc.repo.destroy import destroy
    from dvc.repo.install import install
    from dvc.repo.add import add
    from dvc.repo.remove import remove
    from dvc.repo.lock import lock as lock_stage
    from dvc.repo.move import move
    from dvc.repo.run import run
    from dvc.repo.imp import imp
    from dvc.repo.imp_url import imp_url
    from dvc.repo.reproduce import reproduce
    from dvc.repo.checkout import _checkout
    from dvc.repo.push import push
    from dvc.repo.fetch import _fetch
    from dvc.repo.pull import pull
    from dvc.repo.status import status
    from dvc.repo.gc import gc
    from dvc.repo.commit import commit
    from dvc.repo.diff import diff
    from dvc.repo.brancher import brancher
    from dvc.repo.get import get
    from dvc.repo.get_url import get_url
    from dvc.repo.update import update

    def __init__(self, root_dir=None):
        from dvc.state import State
        from dvc.lock import make_lock
        from dvc.scm import SCM
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.repo.metrics import Metrics
        from dvc.scm.tree import WorkingTree
        from dvc.repo.tag import Tag
        from dvc.utils import makedirs

        root_dir = self.find_root(root_dir)

        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)

        self.scm = SCM(self.root_dir)

        self.tree = WorkingTree(self.root_dir)

        self.tmp_dir = os.path.join(self.dvc_dir, "tmp")
        makedirs(self.tmp_dir, exist_ok=True)

        hardlink_lock = self.config.config["core"].get("hardlink_lock", False)
        self.lock = make_lock(
            os.path.join(self.dvc_dir, "lock"),
            tmp_dir=os.path.join(self.dvc_dir, "tmp"),
            hardlink_lock=hardlink_lock,
            friendly=True,
        )

        # NOTE: storing state and link_state in the repository itself to avoid
        # any possible state corruption in 'shared cache dir' scenario.
        self.state = State(self, self.config.config)

        core = self.config.config[Config.SECTION_CORE]

        level = core.get(Config.SECTION_CORE_LOGLEVEL)
        if level:
            logger.setLevel(level.upper())

        self.cache = Cache(self)
        self.cloud = DataCloud(self)

        self.metrics = Metrics(self)
        self.tag = Tag(self)

        self._ignore()

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
        return "Repo: '{root_dir}'".format(root_dir=self.root_dir)

    @classmethod
    def find_root(cls, root=None):
        if root is None:
            root = os.getcwd()
        else:
            root = os.path.abspath(os.path.realpath(root))

        while True:
            dvc_dir = os.path.join(root, cls.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            if os.path.ismount(root):
                break
            root = os.path.dirname(root)
        raise NotDvcRepoError(root)

    @classmethod
    def find_dvc_dir(cls, root=None):
        root_dir = cls.find_root(root)
        return os.path.join(root_dir, cls.DVC_DIR)

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False):
        from dvc.repo.init import init

        init(root_dir=root_dir, no_scm=no_scm, force=force)
        return Repo(root_dir)

    def unprotect(self, target):
        return self.cache.local.unprotect(PathInfo(target))

    def _ignore(self):
        from dvc.updater import Updater

        updater = Updater(self.dvc_dir)

        flist = (
            [self.config.config_local_file, updater.updater_file]
            + [self.lock.lockfile, updater.lock.lockfile, self.tmp_dir]
            + self.state.files
        )

        if path_isin(self.cache.local.cache_dir, self.root_dir):
            flist += [self.cache.local.cache_dir]

        self.scm.ignore_list(flist)

    def check_modified_graph(self, new_stages):
        """Generate graph including the new stage to check for errors"""
        self._collect_graph(self.stages + new_stages)

    def collect(self, target, with_deps=False, recursive=False, graph=None):
        import networkx as nx
        from dvc.stage import Stage

        G = graph or self.graph

        if not target:
            return get_stages(G)

        target = os.path.abspath(target)

        if recursive and os.path.isdir(target):
            attrs = nx.get_node_attributes(G, "stage")
            nodes = [node for node in nx.dfs_postorder_nodes(G)]

            ret = []
            for node in nodes:
                stage = attrs[node]
                if path_isin(stage.path, target):
                    ret.append(stage)
            return ret

        stage = Stage.load(self, target)
        if not with_deps:
            return [stage]

        node = relpath(stage.path, self.root_dir)
        pipeline = get_pipeline(get_pipelines(G), node)

        return [
            pipeline.node[n]["stage"]
            for n in nx.dfs_postorder_nodes(pipeline, node)
        ]

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
    ):
        """Get the stages related to the given target and collect
        the `info` of its outputs.

        This is useful to know what files from the cache are _in use_
        (namely, a file described as an output on a stage).

        The scope is, by default, the working directory, but you can use
        `all_branches` or `all_tags` to expand scope.

        Returns:
            A dictionary with Schemes (representing output's location) as keys,
            and a list with the outputs' `dumpd` as values.
        """
        from dvc.cache import NamedCache

        cache = NamedCache()

        for branch in self.brancher(
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
        ):
            if targets:
                stages = []
                for target in targets:
                    collected = self.collect(
                        target, recursive=recursive, with_deps=with_deps
                    )
                    stages.extend(collected)
            else:
                stages = self.stages

            for stage in stages:
                if stage.is_repo_import:
                    dep, = stage.deps
                    cache.external[dep.repo_pair].add(dep.def_path)
                    continue

                for out in stage.outs:
                    used_cache = out.get_used_cache(
                        remote=remote, force=force, jobs=jobs
                    )
                    suffix = "({})".format(branch) if branch else ""
                    cache.update(used_cache, suffix=suffix)

        return cache

    def _collect_graph(self, stages=None):
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
        from dvc.exceptions import (
            OutputDuplicationError,
            StagePathAsOutputError,
            OverlappingOutputPathsError,
        )

        G = nx.DiGraph()
        stages = stages or self.collect_stages()
        stages = [stage for stage in stages if stage]
        outs = {}

        for stage in stages:
            for out in stage.outs:
                if out.path_info in outs:
                    stages = [stage.relpath, outs[out.path_info].stage.relpath]
                    raise OutputDuplicationError(str(out), stages)
                outs[out.path_info] = out

        for stage in stages:
            for out in stage.outs:
                for p in out.path_info.parents:
                    if p in outs:
                        raise OverlappingOutputPathsError(outs[p], out)

        for stage in stages:
            stage_path_info = PathInfo(stage.path)
            for p in chain([stage_path_info], stage_path_info.parents):
                if p in outs:
                    raise StagePathAsOutputError(stage.wdir, stage.relpath)

        for stage in stages:
            node = relpath(stage.path, self.root_dir)

            G.add_node(node, stage=stage)

            for dep in stage.deps:
                if dep.path_info is None:
                    continue

                for out in outs:
                    if out.overlaps(dep.path_info):
                        dep_stage = outs[out].stage
                        dep_node = relpath(dep_stage.path, self.root_dir)
                        G.add_node(dep_node, stage=dep_stage)
                        G.add_edge(node, dep_node)

        check_acyclic(G)

        return G

    @cached_property
    def graph(self):
        return self._collect_graph()

    @cached_property
    def pipelines(self):
        return get_pipelines(self.graph)

    @staticmethod
    def _filter_out_dirs(dirs, outs, root_dir):
        def filter_dirs(dname):
            path = os.path.join(root_dir, dname)
            for out in outs:
                if path == os.path.normpath(out):
                    return False
            return True

        return list(filter(filter_dirs, dirs))

    def collect_stages(self):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large repos, this could be an expensive
              operation. Consider using some memoization.
        """
        from dvc.stage import Stage

        stages = []
        outs = []

        for root, dirs, files in self.tree.walk(
            self.root_dir, dvcignore=self.dvcignore
        ):
            for fname in files:
                path = os.path.join(root, fname)
                if not Stage.is_valid_filename(path):
                    continue
                stage = Stage.load(self, path)
                for out in stage.outs:
                    if out.scheme == "local":
                        outs.append(out.fspath + out.sep)
                stages.append(stage)

            dirs[:] = self._filter_out_dirs(dirs, outs, root)

        return stages

    @cached_property
    def stages(self):
        return get_stages(self.graph)

    def find_outs_by_path(self, path, outs=None, recursive=False):
        if not outs:
            outs = [out for stage in self.stages for out in stage.outs]

        abs_path = os.path.abspath(path)
        is_dir = self.tree.isdir(abs_path)

        def func(out):
            if out.scheme == "local" and out.fspath == abs_path:
                return True

            if is_dir and recursive and out.path_info.isin(abs_path):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path, self)

        return matched

    def find_out_by_relpath(self, relpath):
        path = os.path.join(self.root_dir, relpath)
        out, = self.find_outs_by_path(path)
        return out

    def is_dvc_internal(self, path):
        path_parts = os.path.normpath(path).split(os.path.sep)
        return self.DVC_DIR in path_parts

    @contextmanager
    def open(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file descriptor"""
        cause = None
        try:
            out, = self.find_outs_by_path(path)
        except OutputNotFoundError as e:
            out = None
            cause = e

        if out and out.use_cache:
            try:
                with self._open_cached(out, remote, mode, encoding) as fd:
                    yield fd
                return
            except FileNotFoundError as e:
                raise FileMissingError(relpath(path, self.root_dir), cause=e)

        if self.tree.exists(path):
            with self.tree.open(path, mode, encoding) as fd:
                yield fd
            return

        raise FileMissingError(relpath(path, self.root_dir), cause=cause)

    def _open_cached(self, out, remote=None, mode="r", encoding=None):
        if out.isdir():
            raise ValueError("Can't open a dir")

        cache_file = self.cache.local.checksum_to_path_info(out.checksum)
        cache_file = fspath_py35(cache_file)

        if os.path.exists(cache_file):
            return _open(cache_file, mode=mode, encoding=encoding)

        try:
            remote_obj = self.cloud.get_remote(remote)
            remote_info = remote_obj.checksum_to_path_info(out.checksum)
            return remote_obj.open(remote_info, mode=mode, encoding=encoding)
        except RemoteActionNotImplemented:
            with self.state:
                cache_info = out.get_used_cache(remote=remote)
                self.cloud.pull(cache_info, remote=remote)

            return _open(cache_file, mode=mode, encoding=encoding)

    @cached_property
    def dvcignore(self):
        return DvcIgnoreFilter(self.root_dir, self.tree)

    def close(self):
        self.scm.close()

    @locked
    def checkout(self, *args, **kwargs):
        return self._checkout(*args, **kwargs)

    @locked
    def fetch(self, *args, **kwargs):
        return self._fetch(*args, **kwargs)

    def _reset(self):
        self.__dict__.pop("graph", None)
        self.__dict__.pop("stages", None)
        self.__dict__.pop("pipelines", None)
        self.__dict__.pop("dvcignore", None)
