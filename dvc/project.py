import os
import csv
import stat
import json
import networkx as nx
from jsonpath_rw import parse

import dvc.output as Output
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.stage import Stage
from dvc.config import Config
from dvc.state import LinkState, State
from dvc.lock import Lock
from dvc.scm import SCM, Base
from dvc.cache import Cache
from dvc.data_cloud import DataCloud
from dvc.system import System
from dvc.updater import Updater


class InitError(DvcException):
    def __init__(self, msg):
        super(InitError, self).__init__(msg)


class NotDvcFileError(DvcException):
    def __init__(self, path):
        msg = '\'{}\' is not a DVC file'.format(path)
        p = path + Stage.STAGE_FILE_SUFFIX
        if Stage.is_stage_file(p):
            msg += '. Maybe you meant \'{}\'?'.format(p)
        super(NotDvcFileError, self).__init__(msg)


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name, ex):
        self.path = dvc_file_name
        msg = 'Failed to reproduce \'{}\''.format(dvc_file_name)
        super(ReproductionError, self).__init__(msg, cause=ex)


class Project(object):
    DVC_DIR = '.dvc'

    def __init__(self, root_dir):
        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)
        self.scm = SCM(self.root_dir)
        self.lock = Lock(self.dvc_dir)
        # NOTE: storing state and link_state in the repository itself to avoid
        # any possible state corruption in 'shared cache dir' scenario.
        self.state = State(self)
        self.link_state = LinkState(self)
        self.logger = Logger(self.config._config[Config.SECTION_CORE].get(Config.SECTION_CORE_LOGLEVEL, None))
        self.cache = Cache(self)
        self.cloud = DataCloud(self, config=self.config._config)
        self.updater = Updater(self.dvc_dir)

        self._ignore()

        self.updater.check()

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False):
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

        scm = SCM(root_dir)
        if type(scm) == Base and not no_scm:
            msg = "{} is not tracked by any supported scm tool(e.g. git).".format(root_dir)
            raise InitError(msg)

        os.mkdir(dvc_dir)

        config = Config.init(dvc_dir)
        proj = Project(root_dir)

        scm.add([config.config_file])
        if scm.ignore_file():
            scm.add([os.path.join(dvc_dir, scm.ignore_file())])

        return proj

    def _ignore(self):
        l = [self.state.state_file,
             self.state._lock_file.lock_file,
             self.link_state.state_file,
             self.link_state._lock_file.lock_file,
             self.lock.lock_file,
             self.config.config_local_file,
             self.updater.updater_file]

        if self.cache.local.cache_dir.startswith(self.root_dir):
            l += [self.cache.local.cache_dir]

        self.scm.ignore_list(l)

    def install(self):
        self.scm.install()

    def to_dvc_path(self, path):
        return os.path.relpath(path, self.root_dir)

    def add(self, fname):
        out = os.path.basename(os.path.normpath(fname))
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

    def remove(self, target, outs_only=False):
        if not Stage.is_stage_file(target):
            raise NotDvcFileError(target)

        stage = Stage.load(self, target)
        if outs_only:
            stage.remove_outs()
        else:
            stage.remove()

        return stage

    def lock_stage(self, target, unlock=False):
        if not Stage.is_stage_file(target):
            raise NotDvcFileError(target)

        stage = Stage.load(self, target)
        stage.locked = False if unlock else True
        stage.dump()

        return stage

    def move(self, from_path, to_path):
        from_out = Output.loads_from(Stage(self, cwd=os.curdir), [from_path])[0]

        found = False
        for stage in self.stages():
            for out in stage.outs:
                if out.path != from_out.path:
                    continue

                if not stage.is_data_source:
                    raise DvcException('Dvcfile \'{}\' is not a data source.'.format(stage.rel_path))

                found = True
                to_out = Output.loads_from(out.stage, [to_path], out.cache, out.metric)[0]
                out.move(to_out)

                stage_base = os.path.basename(stage.path).rstrip(Stage.STAGE_FILE_SUFFIX)
                stage_dir = os.path.dirname(stage.path)
                from_base = os.path.basename(from_path)
                to_base = os.path.basename(to_path)
                if stage_base == from_base:
                    os.unlink(stage.path)
                    stage.path = os.path.join(stage_dir, to_base + Stage.STAGE_FILE_SUFFIX)

            stage.dump()

        if not found:
            raise DvcException('Unable to find dvcfile with output \'{}\''.format(from_path))

    def run(self,
            cmd=None,
            deps=[],
            outs=[],
            outs_no_cache=[],
            metrics_no_cache=[],
            fname=Stage.STAGE_FILE,
            cwd=os.curdir,
            no_exec=False):
        stage = Stage.loads(project=self,
                            fname=fname,
                            cmd=cmd,
                            cwd=cwd,
                            outs=outs,
                            outs_no_cache=outs_no_cache,
                            metrics_no_cache=metrics_no_cache,
                            deps=deps)
        if not no_exec:
            stage.run()
        stage.dump()
        return stage

    def imp(self, url, out):
        stage_fname = out + Stage.STAGE_FILE_SUFFIX
        cwd = os.path.dirname(os.path.abspath(out))
        stage = Stage.loads(project=self,
                            cmd=None,
                            deps=[url],
                            outs=[out],
                            fname=stage_fname,
                            cwd=cwd)

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
            raise NotDvcFileError(target)

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

    def _cleanup_unused_links(self, all_stages):
        used = []
        for stage in all_stages:
            for out in stage.outs:
                used.append(out.path)
        self.link_state.remove_unused(used)

    def checkout(self, target=None):
        all_stages = self.stages()

        if target:
            if not Stage.is_stage_file(target):
                raise NotDvcFileError(target)
            stages = [Stage.load(self, target)]
        else:
            stages = all_stages

        self._cleanup_unused_links(all_stages)

        for stage in stages:
            stage.checkout()

    def _used_cache(self, target=None, all_branches=False):
        cache = {}
        cache['local'] = []
        cache['s3'] = []
        cache['gs'] = []
        cache['hdfs'] = []
        cache['ssh'] = []

        for branch in self.scm.brancher(all_branches=all_branches):
            if target:
                stages = [Stage.load(self, target)]
            else:
                stages = self.stages()

            for stage in stages:
                for out in stage.outs:
                    if not out.use_cache or not out.info:
                        continue

                    cache[out.path_info['scheme']] += [out.info]

        return cache

    def gc(self, all_branches=False):
        clist = self._used_cache(target=None, all_branches=all_branches)
        self.cache.local.gc(clist['local'])

        if self.cache.s3:
            self.cache.s3.gc(clist['s3'])

        if self.cache.gs:
            self.cache.gs.gc(clist['gs'])

        if self.cache.ssh:
            self.cache.ssh.gc(clist['ssh'])

        if self.cache.hdfs:
            self.cache.hdfs.gc(clist['hdfs'])

    def push(self, target=None, jobs=1, remote=None, all_branches=False):
        self.cloud.push(self._used_cache(target, all_branches)['local'], jobs, remote=remote)

    def fetch(self, target=None, jobs=1, remote=None, all_branches=False):
        self.cloud.pull(self._used_cache(target, all_branches)['local'], jobs, remote=remote)

    def pull(self, target=None, jobs=1, remote=None, all_branches=False):
        self.fetch(target, jobs, remote=remote, all_branches=all_branches)
        self.checkout(target=target)

    def _local_status(self, target=None):
        status = {}

        if target:
            stages = [Stage.load(self, target)]
        else:
            stages = self.stages()

        for stage in stages:
            status.update(stage.status())

        return status

    def _cloud_status(self, target=None, jobs=1, remote=None):
        import dvc.remote.base as cloud

        status = {}
        for md5, ret in self.cloud.status(self._used_cache(target)['local'], jobs, remote=remote):
            if ret == cloud.STATUS_OK:
                continue

            prefix_map = {
                cloud.STATUS_DELETED: 'deleted',
                cloud.STATUS_NEW: 'new',
            }

            status[md5] = prefix_map[ret]

        return status

    def status(self, target=None, jobs=1, cloud=False, remote=None):
        if cloud:
            return self._cloud_status(target, jobs, remote=remote)
        return self._local_status(target)

    def _read_metric_json(self, fd, json_path):
        parser = parse(json_path)
        return [x.value for x in parser.find(json.load(fd))]

    def _do_read_metric_tsv(self, reader, row, col):
        if col != None and row != None:
            return [reader[row][col]]
        elif col != None:
            return [r[col] for r in reader]
        elif row != None:
            return reader[row]
        return None

    def _read_metric_htsv(self, fd, htsv_path):
        col, row = htsv_path.split(',')
        row = int(row)
        reader = list(csv.DictReader(fd, delimiter='\t'))
        return self._do_read_metric_tsv(reader, row, col)

    def _read_metric_tsv(self, fd, tsv_path):
        col, row = tsv_path.split(',')
        row = int(row)
        col = int(col)
        reader = list(csv.reader(fd, delimiter='\t'))
        return self._do_read_metric_tsv(reader, row, col)

    def _read_metric(self, path, json_path=None, tsv_path=None, htsv_path=None):
        ret = None

        if not os.path.exists(path):
            return ret

        try: 
            with open(path, 'r') as fd:
                if json_path:
                    ret = self._read_metric_json(fd, json_path)
                elif tsv_path:
                    ret = self._read_metric_tsv(fd, tsv_path)
                elif htsv_path:
                    ret = self._read_metric_htsv(fd, htsv_path)
                else:
                    ret = fd.read()
        except Exception as exc:
            self.logger.error('Unable to read metric in \'{}\''.format(path), exc)

        return ret

    def metrics_show(self, path=None, json_path=None, tsv_path=None, htsv_path=None, all_branches=False):
        res = {}
        for branch in self.scm.brancher(all_branches=all_branches):
            metrics = filter(lambda o: o.metric, self.outs())
            fnames = [path] if path else map(lambda o: o.path, metrics)
            for fname in fnames:
                rel = os.path.relpath(fname)
                metric = self._read_metric(fname,
                                           json_path=json_path,
                                           tsv_path=tsv_path,
                                           htsv_path=htsv_path)
                if not metric:
                    continue

                if branch not in res:
                    res[branch] = {}

                res[branch][rel] = metric

        for branch, val in res.items():
            if all_branches:
                self.logger.info('{}:'.format(branch))
            for fname, metric in val.items():
                self.logger.info('\t{}: {}'.format(fname, metric))

        return res

    def _metrics_modify(self, path, val):
        apath = os.path.abspath(path)
        for stage in self.stages():
            for out in stage.outs:
                if apath != out.path:
                    continue

                if out.path_info['scheme'] != 'local':
                    msg = 'Output \'{}\' scheme \'{}\' is not supported for metrics'
                    raise DvcException(msg.format(out.path, out.path_info['scheme']))

                if out.use_cache:
                    msg = 'Cached output \'{}\' is not supported for metrics'
                    raise DvcException(msg.format(out.rel_path))

                out.metric = val

            stage.dump()

    def metrics_add(self, path):
        self._metrics_modify(path, True)

    def metrics_remove(self, path):
        self._metrics_modify(path, False)

    def graph(self):
        G = nx.DiGraph()
        stages = self.stages()
        outs = self.outs()

        for stage in stages:
            node = os.path.relpath(stage.path, self.root_dir)
            G.add_node(node, stage=stage)
            if stage.locked:
                continue
            for dep in stage.deps:
                for out in outs:
                    if out.path != dep.path and not dep.path.startswith(out.path + out.sep):
                        continue

                    dep_stage = out.stage
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
