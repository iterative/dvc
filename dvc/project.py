import os
import stat
import networkx as nx

import dvc.cloud.base as cloud

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.stage import Stage, Output
from dvc.config import Config
from dvc.state import State
from dvc.lock import Lock
from dvc.scm import SCM
from dvc.cache import Cache
from dvc.cloud.data_cloud import DataCloud
from dvc.system import System


class StageNotFoundError(DvcException):
    def __init__(self, path):
        msg = 'Stage file {} does not exist'.format(path)
        super(StageNotFoundError, self).__init__(msg)


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name, ex):
        msg = 'Failed to reproduce \'{}\''.format(dvc_file_name)
        super(ReproductionError, self).__init__(msg, cause=ex)


class Project(object):
    DVC_DIR = '.dvc'

    def __init__(self, root_dir):
        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.scm = SCM(self.root_dir)
        self.lock = Lock(self.dvc_dir)
        self.cache = Cache(self.dvc_dir)
        self.state = State(self.root_dir, self.dvc_dir)
        self.config = Config(self.dvc_dir)
        self.logger = Logger(self.config._config)
        self.cloud = DataCloud(self.cache, self.config._config)

    @staticmethod
    def init(root_dir=os.curdir):
        """
        Initiate dvc project in directory.

        Args:
            root_dir: Path to project's root directory.

        Returns:
            Project instance.

        Raises:
            KeyError: Raises an exception.
        """
        root_dir = os.path.abspath(root_dir)
        dvc_dir = os.path.join(root_dir, Project.DVC_DIR)
        os.mkdir(dvc_dir)

        config = Config.init(dvc_dir)
        cache = Cache.init(dvc_dir)
        state = State.init(root_dir, dvc_dir)
        lock = Lock(dvc_dir)

        scm = SCM(root_dir)
        scm.ignore_list([cache.cache_dir,
                         state.state_file,
                         lock.lock_file])

        ignore_file = os.path.join(dvc_dir, scm.ignore_file())
        scm.add([config.config_file, ignore_file])

        return Project(root_dir)

    def to_dvc_path(self, path):
        return os.path.relpath(path, self.root_dir)

    def add(self, fname):
        out = os.path.basename(fname)
        stage_fname = out + Stage.STAGE_FILE_SUFFIX
        cwd = os.path.dirname(os.path.abspath(fname))
        stage = Stage.loads(project=self,
                            cmd=None,
                            deps=[],
                            outs=[out],
                            fname=stage_fname,
                            cwd=cwd)

        stage.save()
        stage.dump()
        return stage

    def remove(self, target):
        if not Stage.is_stage_file(target):
            raise StageNotFoundError(target)

        stage = Stage.load(self, target)
        for out in stage.outs:
            out.remove()

        return stage

    def run(self,
            cmd=None,
            deps=[],
            outs=[],
            outs_no_cache=[],
            fname=Stage.STAGE_FILE,
            cwd=os.curdir,
            no_exec=False):
        stage = Stage.loads(project=self,
                            fname=fname,
                            cmd=cmd,
                            cwd=cwd,
                            outs=outs,
                            outs_no_cache=outs_no_cache,
                            deps=deps)
        if not no_exec:
            stage.run()
        stage.dump()
        return stage

    def _reproduce_stage(self, stages, node, force):
        stage = stages[node].reproduce(force=force)
        if not stage:
            return []
        stage.dump()
        return [stage]

    def reproduce(self, target, recursive=True, force=False):
        stages = nx.get_node_attributes(self.graph(), 'stage')
        node = os.path.relpath(os.path.abspath(target), self.root_dir)
        if node not in stages:
            raise StageNotFoundError(target)

        if recursive:
            return self._reproduce_stages(stages, node, force)

        return self._reproduce_stage(stages, node, force)

    def _reproduce_stages(self, stages, node, force):
        result = []
        for n in nx.dfs_postorder_nodes(self.graph(), node):
            try:
                result += self._reproduce_stage(stages, n, force)
            except Exception as ex:
                raise ReproductionError(stages[n].relpath, ex)
        return result

    def _remove_untracked_hardlinks(self):
        untracked = self.scm.untracked_files()
        cache = dict((System.inode(c), c) for c in self.cache.all())
        for file in untracked:
            inode = System.inode(file)
            if inode not in cache.keys():
                continue

            Logger.info(u'Remove \'{}\''.format(file))
            os.remove(file)

            dir = os.path.dirname(file)
            if len(dir) != 0 and not os.listdir(dir):
                Logger.info(u'Remove empty directory \'{}\''.format(dir))
                os.removedirs(dir)

    def checkout(self):
        self._remove_untracked_hardlinks()
        for stage in self.stages():
            stage.checkout()

    def _used_cache(self, target=None):
        cache_set = set()

        if target:
            stages = [Stage.load(self, target)]
        else:
            stages = self.stages()

        for stage in stages:
            for out in stage.outs:
                if not out.use_cache or not out.cache:
                    continue
                cache_set |= set([out.cache])
                if out.is_dir_cache(out.cache) and os.path.isfile(out.cache):
                    dir_cache = out.dir_cache()
                    cache_set |= set(dir_cache.values())

        return list(cache_set)

    def gc(self):
        clist = self._used_cache()
        for cache in self.cache.all():
            if cache in clist:
                continue
            os.unlink(cache)
            self.logger.info(u'\'{}\' was removed'.format(self.to_dvc_path(cache)))

    def push(self, target=None, jobs=1):
        return self.cloud.push(self._used_cache(target), jobs)

    def fetch(self, target=None, jobs=1):
        return self.cloud.pull(self._used_cache(target), jobs)

    def pull(self, target=None, jobs=1):
        ret = self.fetch(target, jobs)
        self.checkout()
        return ret

    def _local_status(self, target=None):
        status = {}

        if target:
            stages = [Stage.load(self, target)]
        else:
            stages = self.stages()

        for stage in self.stages():
            status.update(stage.status())

        return status

    def _cloud_status(self, target=None, jobs=1):
        status = {}
        for target, ret in self.cloud.status(self._used_cache(target), jobs):
            if ret == cloud.STATUS_UNKNOWN or ret == cloud.STATUS_OK:
                continue

            prefix_map = {
                cloud.STATUS_DELETED: 'deleted',
                cloud.STATUS_MODIFIED: 'modified',
                cloud.STATUS_NEW: 'new',
            }

            path = os.path.relpath(target, self.cache.cache_dir)

            status[path] = prefix_map[ret]

        return status

    def status(self, target=None, jobs=1, cloud=False):
        if cloud:
            return self._cloud_status(target, jobs)
        return self._local_status(target)

    def graph(self):
        G = nx.DiGraph()

        for stage in self.stages():
            node = os.path.relpath(stage.path, self.root_dir)
            G.add_node(node, stage=stage)
            for dep in stage.deps:
                dep_stage = dep.stage()
                if not dep_stage:
                    continue
                dep_node = os.path.relpath(dep_stage.path, self.root_dir)
                G.add_node(dep_node, stage=dep_stage)
                G.add_edge(node, dep_node)

        return G

    def stages(self):
        stages = []
        for root, dirs, files in os.walk(self.root_dir):
            for fname in files:
                path = os.path.join(root, fname)
                if not Stage.is_stage_file(path):
                    continue
                stages.append(Stage.load(self, path))
        return stages

    def outs(self):
        outs = []
        for stage in self.stages():
            outs += stage.outs
        return outs
