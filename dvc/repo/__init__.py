from __future__ import unicode_literals

import os
import logging

import dvc.prompt as prompt

from dvc.exceptions import (
    DvcException,
    NotDvcRepoError,
    OutputNotFoundError,
    TargetNotDirectoryError,
)
from dvc.ignore import DvcIgnoreFileHandler
from dvc.path.local import PathLOCAL

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

    def __init__(self, root_dir=None):
        from dvc.config import Config
        from dvc.state import State
        from dvc.lock import Lock
        from dvc.scm import SCM
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.updater import Updater
        from dvc.repo.metrics import Metrics
        from dvc.scm.tree import WorkingTree
        from dvc.repo.tag import Tag
        from dvc.repo.pkg import Pkg

        root_dir = self.find_root(root_dir)

        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)

        self.tree = WorkingTree(self.root_dir)

        self.scm = SCM(self.root_dir, repo=self)
        self.lock = Lock(self.dvc_dir)
        # NOTE: storing state and link_state in the repository itself to avoid
        # any possible state corruption in 'shared cache dir' scenario.
        self.state = State(self, self.config.config)

        core = self.config.config[Config.SECTION_CORE]

        level = core.get(Config.SECTION_CORE_LOGLEVEL)
        if level:
            logger.setLevel(level.upper())

        self.cache = Cache(self)
        self.cloud = DataCloud(self, config=self.config.config)
        self.updater = Updater(self.dvc_dir)

        self.metrics = Metrics(self)
        self.tag = Tag(self)
        self.pkg = Pkg(self)

        self._ignore()

        self.updater.check()

    def __repr__(self):
        return "Repo: '{root_dir}'".format(root_dir=self.root_dir)

    @staticmethod
    def find_root(root=None):
        if root is None:
            root = os.getcwd()
        else:
            root = os.path.abspath(os.path.realpath(root))

        while True:
            dvc_dir = os.path.join(root, Repo.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            if os.path.ismount(root):
                break
            root = os.path.dirname(root)
        raise NotDvcRepoError(root)

    @staticmethod
    def find_dvc_dir(root=None):
        root_dir = Repo.find_root(root)
        return os.path.join(root_dir, Repo.DVC_DIR)

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False):
        from dvc.repo.init import init

        init(root_dir=root_dir, no_scm=no_scm, force=force)
        return Repo(root_dir)

    def unprotect(self, target):
        path_info = PathLOCAL(path=target)
        return self.cache.local.unprotect(path_info)

    def _ignore(self):
        flist = [
            self.state.state_file,
            self.lock.lock_file,
            self.config.config_local_file,
            self.updater.updater_file,
            self.updater.lock.lock_file,
        ] + self.state.temp_files

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

        if not target or recursive:
            return self.active_stages(target)

        stage = Stage.load(self, target)
        if not with_deps:
            return [stage]

        node = os.path.relpath(stage.path, self.root_dir)
        G = self._get_pipeline(node)

        ret = []
        for n in nx.dfs_postorder_nodes(G, node):
            ret.append(G.node[n]["stage"])

        return ret

    def _collect_dir_cache(
        self, out, branch=None, remote=None, force=False, jobs=None
    ):
        info = out.dumpd()
        ret = [info]
        r = out.remote
        md5 = info[r.PARAM_CHECKSUM]

        if self.cache.local.changed_cache_file(md5):
            try:
                self.cloud.pull(
                    ret, jobs=jobs, remote=remote, show_checksums=False
                )
            except DvcException as exc:
                msg = "Failed to pull cache for '{}': {}"
                logger.debug(msg.format(out, exc))

        if self.cache.local.changed_cache_file(md5):
            msg = (
                "Missing cache for directory '{}'. "
                "Cache for files inside will be lost. "
                "Would you like to continue? Use '-f' to force. "
            )
            if not force and not prompt.confirm(msg):
                raise DvcException(
                    "unable to fully collect used cache"
                    " without cache for directory '{}'".format(out)
                )
            else:
                return ret

        for i in out.dir_cache:
            i["branch"] = branch
            i[r.PARAM_PATH] = os.path.join(
                info[r.PARAM_PATH], i[r.PARAM_RELPATH]
            )
            ret.append(i)

        return ret

    def _collect_used_cache(
        self, out, branch=None, remote=None, force=False, jobs=None
    ):
        if not out.use_cache or not out.info:
            if not out.info:
                logger.warning(
                    "Output '{}'({}) is missing version "
                    "info. Cache for it will not be collected. "
                    "Use dvc repro to get your pipeline up to "
                    "date.".format(out, out.stage)
                )
            return []

        info = out.dumpd()
        info["branch"] = branch
        ret = [info]

        if out.scheme != "local":
            return ret

        if not out.is_dir_checksum:
            return ret

        return self._collect_dir_cache(
            out, branch=branch, remote=remote, force=force, jobs=jobs
        )

    def used_cache(
        self,
        target=None,
        all_branches=False,
        active=True,
        with_deps=False,
        all_tags=False,
        remote=None,
        force=False,
        jobs=None,
        recursive=False,
    ):
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
            if target:
                if recursive:
                    stages = self.stages(target)
                else:
                    stages = self.collect(target, with_deps=with_deps)
            elif active:
                stages = self.active_stages()
            else:
                stages = self.stages()

            for stage in stages:
                if active and not target and stage.locked:
                    logger.warning(
                        "DVC file '{path}' is locked. Its dependencies are"
                        " not going to be pushed/pulled/fetched.".format(
                            path=stage.relpath
                        )
                    )

                for out in stage.outs:
                    scheme = out.path_info.scheme
                    cache[scheme].extend(
                        self._collect_used_cache(
                            out,
                            branch=branch,
                            remote=remote,
                            force=force,
                            jobs=jobs,
                        )
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
        outs = []

        for stage in stages:
            for out in stage.outs:
                existing = []
                for o in outs:
                    if o.path == out.path:
                        existing.append(o.stage)

                    in_o_dir = out.path.startswith(o.path + o.sep)
                    in_out_dir = o.path.startswith(out.path + out.sep)
                    if in_o_dir or in_out_dir:
                        raise OverlappingOutputPathsError(o, out)

                if existing:
                    stages = [stage.relpath, existing[0].relpath]
                    raise OutputDuplicationError(out.path, stages)

                outs.append(out)

        for stage in stages:
            path_dir = os.path.dirname(stage.path) + os.sep
            for out in outs:
                if path_dir.startswith(out.path + os.sep):
                    raise StagePathAsOutputError(stage.wdir, stage.relpath)

        for stage in stages:
            node = os.path.relpath(stage.path, self.root_dir)

            G.add_node(node, stage=stage)
            G_active.add_node(node, stage=stage)

            for dep in stage.deps:
                for out in outs:
                    if (
                        out.path != dep.path
                        and not dep.path.startswith(out.path + out.sep)
                        and not out.path.startswith(dep.path + dep.sep)
                    ):
                        continue

                    dep_stage = out.stage
                    dep_node = os.path.relpath(dep_stage.path, self.root_dir)
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

        ignore_file_handler = DvcIgnoreFileHandler(self.tree)
        for root, dirs, files in self.tree.walk(
            from_directory, ignore_file_handler=ignore_file_handler
        ):
            for fname in files:
                path = os.path.join(root, fname)
                if not Stage.is_valid_filename(path):
                    continue
                stage = Stage.load(self, path)
                for out in stage.outs:
                    outs.append(out.path + out.sep)
                stages.append(stage)

            def filter_dirs(dname, root=root):
                path = os.path.join(root, dname)
                if path in (self.dvc_dir, self.scm.dir):
                    return False
                for out in outs:
                    if path == os.path.normpath(out) or path.startswith(out):
                        return False
                return True

            dirs[:] = list(filter(filter_dirs, dirs))

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
            if out.path == abs_path:
                return True

            if is_dir and recursive and out.path.startswith(abs_path + os.sep):
                return True

            return False

        matched = list(filter(func, outs))
        if not matched:
            raise OutputNotFoundError(path)

        return matched

    def is_dvc_internal(self, path):
        path_parts = os.path.normpath(path).split(os.path.sep)
        return self.DVC_DIR in path_parts
