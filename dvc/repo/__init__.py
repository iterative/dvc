from __future__ import unicode_literals

import os
import logging

from itertools import chain

from funcy import cached_property

from dvc.config import Config
from dvc.exceptions import (
    NotDvcRepoError,
    OutputNotFoundError,
    TargetNotDirectoryError,
    OutputFileMissingError,
)
from dvc.ignore import DvcIgnoreFilter
from dvc.path_info import PathInfo
from dvc.utils.compat import open as _open, fspath_py35
from dvc.utils import relpath

logger = logging.getLogger(__name__)


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
    from dvc.repo.checkout import checkout
    from dvc.repo.push import push
    from dvc.repo.fetch import fetch
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
        from dvc.lock import Lock
        from dvc.scm import SCM
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.repo.metrics import Metrics
        from dvc.scm.tree import WorkingTree
        from dvc.repo.tag import Tag

        root_dir = self.find_root(root_dir)

        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)

        self.scm = SCM(self.root_dir, repo=self)

        self.tree = WorkingTree(self.root_dir)

        self.lock = Lock(self.dvc_dir)
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

        flist = [
            self.lock.lock_file,
            self.config.config_local_file,
            updater.updater_file,
            updater.lock.lock_file,
        ] + self.state.files

        if self.cache.local.cache_dir.startswith(self.root_dir):
            flist += [self.cache.local.cache_dir]

        self.scm.ignore_list(flist)

    def check_dag(self, stages):
        """Generate graph including the new stage to check for errors"""
        self.graph(stages=stages)

    @staticmethod
    def _check_cyclic_graph(graph):
        import networkx as nx
        from dvc.exceptions import CyclicGraphError

        cycles = list(nx.simple_cycles(graph))

        if cycles:
            raise CyclicGraphError(cycles[0])

    def _get_pipeline(self, node):
        pipelines = [i for i in self.pipelines() if i.has_node(node)]
        assert len(pipelines) == 1
        return pipelines[0]

    def collect(self, target, with_deps=False, recursive=False):
        import networkx as nx
        from dvc.stage import Stage

        if not target or (recursive and os.path.isdir(target)):
            return self.active_stages(target)

        stage = Stage.load(self, target)
        if not with_deps:
            return [stage]

        node = relpath(stage.path, self.root_dir)
        G = self._get_pipeline(node)

        ret = []
        for n in nx.dfs_postorder_nodes(G, node):
            ret.append(G.node[n]["stage"])

        return ret

    def used_cache(
        self,
        targets=None,
        all_branches=False,
        active=True,
        with_deps=False,
        all_tags=False,
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

        cache = {}
        cache["local"] = []
        cache["s3"] = []
        cache["gs"] = []
        cache["hdfs"] = []
        cache["ssh"] = []
        cache["azure"] = []

        for branch in self.brancher(
            all_branches=all_branches, all_tags=all_tags
        ):
            if targets:
                stages = []
                for target in targets:
                    if recursive and os.path.isdir(target):
                        stages.extend(self.stages(target))
                    else:
                        stages.extend(
                            self.collect(target, with_deps=with_deps)
                        )
            elif active:
                stages = self.active_stages()
            else:
                stages = self.stages()

            for stage in stages:
                if active and not targets and stage.locked:
                    logger.warning(
                        "DVC-file '{path}' is locked. Its dependencies are"
                        " not going to be pushed/pulled/fetched.".format(
                            path=stage.relpath
                        )
                    )

                for out in stage.outs:
                    scheme = out.path_info.scheme
                    used_cache = out.get_used_cache(
                        remote=remote, force=force, jobs=jobs
                    )

                    cache[scheme].extend(
                        dict(entry, branch=branch) for entry in used_cache
                    )

        return cache

    def graph(self, stages=None, from_directory=None):
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
            stages (list): used to build a graph, if None given, use the ones
                on the `from_directory`.

            from_directory (str): directory where to look at for stages, if
                None is given, use the current working directory

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
        G_active = nx.DiGraph()
        stages = stages or self.stages(from_directory, check_dag=False)
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
            G_active.add_node(node, stage=stage)

            for dep in stage.deps:
                if dep.path_info is None:
                    continue

                for out in outs:
                    if (
                        out == dep.path_info
                        or dep.path_info.isin(out)
                        or out.isin(dep.path_info)
                    ):
                        dep_stage = outs[out].stage
                        dep_node = relpath(dep_stage.path, self.root_dir)
                        G.add_node(dep_node, stage=dep_stage)
                        G.add_edge(node, dep_node)
                        if not stage.locked:
                            G_active.add_node(dep_node, stage=dep_stage)
                            G_active.add_edge(node, dep_node)

        self._check_cyclic_graph(G)

        return G, G_active

    def pipelines(self, from_directory=None):
        import networkx as nx

        G, G_active = self.graph(from_directory=from_directory)

        return [
            G.subgraph(c).copy() for c in nx.weakly_connected_components(G)
        ]

    @staticmethod
    def _filter_out_dirs(dirs, outs, root_dir):
        def filter_dirs(dname):
            path = os.path.join(root_dir, dname)
            for out in outs:
                if path == os.path.normpath(out):
                    return False
            return True

        return list(filter(filter_dirs, dirs))

    def stages(self, from_directory=None, check_dag=True):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large repos, this could be an expensive
              operation. Consider using some memoization.
        """
        from dvc.stage import Stage

        if not from_directory:
            from_directory = self.root_dir
        elif not os.path.isdir(from_directory):
            raise TargetNotDirectoryError(from_directory)

        stages = []
        outs = []

        for root, dirs, files in self.tree.walk(
            from_directory, dvcignore=self.dvcignore
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

        if check_dag:
            self.check_dag(stages)

        return stages

    def active_stages(self, from_directory=None):
        import networkx as nx

        stages = []
        for G in self.pipelines(from_directory):
            stages.extend(list(nx.get_node_attributes(G, "stage").values()))
        return stages

    def find_outs_by_path(self, path, outs=None, recursive=False):
        if not outs:
            # there is no `from_directory=path` argument because some data
            # files might be generated to an upper level, and so it is
            # needed to look at all the files (project root_dir)
            stages = self.stages()
            outs = [out for stage in stages for out in stage.outs]

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
            raise OutputNotFoundError(path)

        return matched

    def find_out_by_relpath(self, relpath):
        path = os.path.join(self.root_dir, relpath)
        out, = self.find_outs_by_path(path)
        return out

    def is_dvc_internal(self, path):
        path_parts = os.path.normpath(path).split(os.path.sep)
        return self.DVC_DIR in path_parts

    def open(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file descriptor"""
        out, = self.find_outs_by_path(path)
        if out.isdir():
            raise ValueError("Can't open a dir")

        cache_file = self.cache.local.checksum_to_path_info(out.checksum)
        cache_file = fspath_py35(cache_file)

        with self.state:
            cache_info = out.get_used_cache(remote=remote)
            self.cloud.pull(cache_info, remote=remote)

        # Since pull may just skip with a warning, we need to check it here
        if not os.path.exists(cache_file):
            raise OutputFileMissingError(relpath(path, self.root_dir))

        return _open(cache_file, mode=mode, encoding=encoding)

    @cached_property
    def dvcignore(self):
        return DvcIgnoreFilter(self.root_dir)
