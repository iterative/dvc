import os

from dvc.exceptions import DvcException
from dvc.stage import Stage


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
        from dvc.logger import Logger
        from dvc.config import Config
        from dvc.state import LinkState, State
        from dvc.lock import Lock
        from dvc.scm import SCM
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.updater import Updater

        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)
        self.scm = SCM(self.root_dir)
        self.lock = Lock(self.dvc_dir)
        # NOTE: storing state and link_state in the repository itself to avoid
        # any possible state corruption in 'shared cache dir' scenario.
        self.state = State(self)
        self.link_state = LinkState(self)

        core = self.config._config[Config.SECTION_CORE]
        self.logger = Logger(core.get(Config.SECTION_CORE_LOGLEVEL, None))

        self.cache = Cache(self)
        self.cloud = DataCloud(self, config=self.config._config)
        self.updater = Updater(self.dvc_dir)

        self._ignore()

        self.updater.check()

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False):
        """
        Initiate dvc project in directory.

        Args:
            root_dir: Path to project's root directory.

        Returns:
            Project instance.

        Raises:
            KeyError: Raises an exception.
        """
        import shutil
        from dvc.scm import SCM, Base
        from dvc.config import Config

        root_dir = os.path.abspath(root_dir)
        dvc_dir = os.path.join(root_dir, Project.DVC_DIR)

        scm = SCM(root_dir)
        if type(scm) == Base and not no_scm:
            msg = "{} is not tracked by any supported scm tool(e.g. git)."
            raise InitError(msg.format(root_dir))

        if os.path.isdir(dvc_dir):
            if not force:
                msg = "'{}' exists. Use '-f' to force."
                raise InitError(msg.format(os.path.relpath(dvc_dir)))
            shutil.rmtree(dvc_dir)

        os.mkdir(dvc_dir)

        config = Config.init(dvc_dir)
        proj = Project(root_dir)

        scm.add([config.config_file])
        if scm.ignore_file():
            scm.add([os.path.join(dvc_dir, scm.ignore_file())])

        return proj

    def destroy(self):
        import shutil

        for stage in self.stages():
            stage.remove()

        shutil.rmtree(self.dvc_dir)

    def _ignore(self):
        flist = [self.state.state_file,
                 self.state._lock_file.lock_file,
                 self.link_state.state_file,
                 self.link_state._lock_file.lock_file,
                 self.lock.lock_file,
                 self.config.config_local_file,
                 self.updater.updater_file]

        if self.cache.local.cache_dir.startswith(self.root_dir):
            flist += [self.cache.local.cache_dir]

        self.scm.ignore_list(flist)

    def install(self):
        self.scm.install()

    def to_dvc_path(self, path):
        return os.path.relpath(path, self.root_dir)

    def add(self, fname):
        stage = Stage.loads(project=self,
                            outs=[fname],
                            add=True)

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
        import dvc.output as Output

        from_out = Output.loads_from(Stage(self, cwd=os.curdir),
                                     [from_path])[0]

        found = False
        for stage in self.stages():
            for out in stage.outs:
                if out.path != from_out.path:
                    continue

                if not stage.is_data_source:
                    msg = 'Dvcfile \'{}\' is not a data source.'
                    raise DvcException(msg.format(stage.rel_path))

                found = True
                to_out = Output.loads_from(out.stage,
                                           [to_path],
                                           out.cache,
                                           out.metric)[0]
                out.move(to_out)

                stage_base = os.path.basename(stage.path)
                stage_base = stage_base.rstrip(Stage.STAGE_FILE_SUFFIX)

                stage_dir = os.path.dirname(stage.path)
                from_base = os.path.basename(from_path)
                to_base = os.path.basename(to_path)
                if stage_base == from_base:
                    os.unlink(stage.path)
                    path = to_base + Stage.STAGE_FILE_SUFFIX
                    stage.path = os.path.join(stage_dir, path)

            stage.dump()

        if not found:
            msg = 'Unable to find dvcfile with output \'{}\''
            raise DvcException(msg.format(from_path))

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
        stage = Stage.loads(project=self,
                            cmd=None,
                            deps=[url],
                            outs=[out])

        stage.run()
        stage.dump()
        return stage

    def _reproduce_stage(self, stages, node, force):
        stage = stages[node]

        if stage.locked:
            msg = 'DVC file \'{}\' is locked. Its dependecies are not ' \
                  'going to be reproduced.'
            self.logger.warn(msg.format(stage.relpath))

        stage = stage.reproduce(force=force)
        if not stage:
            return []
        stage.dump()
        return [stage]

    def reproduce(self, target, recursive=True, force=False):
        import networkx as nx

        G = self.graph()[1]
        stages = nx.get_node_attributes(G, 'stage')
        node = os.path.relpath(os.path.abspath(target), self.root_dir)
        if node not in stages:
            raise NotDvcFileError(target)

        if recursive:
            return self._reproduce_stages(G, stages, node, force)

        return self._reproduce_stage(stages, node, force)

    def _reproduce_stages(self, G, stages, node, force):
        import networkx as nx

        result = []
        for n in nx.dfs_postorder_nodes(G, node):
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
        all_stages = self.active_stages()

        if target:
            if not Stage.is_stage_file(target):
                raise NotDvcFileError(target)
            stages = [Stage.load(self, target)]
        else:
            stages = all_stages

        self._cleanup_unused_links(all_stages)

        for stage in stages:
            if stage.locked:
                msg = 'DVC file \'{}\' is locked. Its dependecies are not ' \
                      'going to be checked out.'
                self.logger.warn(msg.format(stage.relpath))

            stage.checkout()

    def _used_cache(self, target=None, all_branches=False, active=True):
        cache = {}
        cache['local'] = []
        cache['s3'] = []
        cache['gs'] = []
        cache['hdfs'] = []
        cache['ssh'] = []

        for branch in self.scm.brancher(all_branches=all_branches):
            if target:
                stages = [Stage.load(self, target)]
            elif active:
                stages = self.active_stages()
            else:
                stages = self.stages()

            for stage in stages:
                if active and not target and stage.locked:
                    msg = 'DVC file \'{}\' is locked. Its dependecies are ' \
                          'not going to be pushed/pulled/fetched.'
                    self.logger.warn(msg.format(stage.relpath))

                for out in stage.outs:
                    if not out.use_cache or not out.info:
                        continue

                    cache[out.path_info['scheme']] += [out.info]

        return cache

    def gc(self, all_branches=False, cloud=False, remote=None):
        clist = self._used_cache(target=None,
                                 all_branches=all_branches,
                                 active=False)
        self.cache.local.gc(clist)

        if self.cache.s3:
            self.cache.s3.gc(clist)

        if self.cache.gs:
            self.cache.gs.gc(clist)

        if self.cache.ssh:
            self.cache.ssh.gc(clist)

        if self.cache.hdfs:
            self.cache.hdfs.gc(clist)

        if self.cache.azure:
            self.cache.azure.gc(clist)

        if cloud:
            self.cloud._get_cloud(remote).gc(clist)

    def push(self, target=None, jobs=1, remote=None, all_branches=False):
        self.cloud.push(self._used_cache(target, all_branches)['local'],
                        jobs,
                        remote=remote)

    def fetch(self, target=None, jobs=1, remote=None, all_branches=False):
        self.cloud.pull(self._used_cache(target, all_branches)['local'],
                        jobs,
                        remote=remote)

    def pull(self, target=None, jobs=1, remote=None, all_branches=False):
        self.fetch(target, jobs, remote=remote, all_branches=all_branches)
        self.checkout(target=target)

    def _local_status(self, target=None):
        status = {}

        if target:
            stages = [Stage.load(self, target)]
        else:
            stages = self.active_stages()

        for stage in stages:
            if stage.locked:
                msg = 'DVC file \'{}\' is locked. Its dependecies are not ' \
                      'going to be shown in status output.'
                self.logger.warn(msg.format(stage.relpath))

            status.update(stage.status())

        return status

    def _cloud_status(self, target=None, jobs=1, remote=None):
        import dvc.remote.base as cloud

        status = {}
        for md5, ret in self.cloud.status(self._used_cache(target)['local'],
                                          jobs,
                                          remote=remote):
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
        import json
        from jsonpath_rw import parse

        parser = parse(json_path)
        return [x.value for x in parser.find(json.load(fd))]

    def _do_read_metric_xsv(self, reader, row, col):
        if col is not None and row is not None:
            return [reader[row][col]]
        elif col is not None:
            return [r[col] for r in reader]
        elif row is not None:
            return reader[row]
        return None

    def _read_metric_hxsv(self, fd, hxsv_path, delimiter):
        import csv

        col, row = hxsv_path.split(',')
        row = int(row)
        reader = list(csv.DictReader(fd, delimiter=delimiter))
        return self._do_read_metric_xsv(reader, row, col)

    def _read_metric_xsv(self, fd, xsv_path, delimiter):
        import csv

        col, row = xsv_path.split(',')
        row = int(row)
        col = int(col)
        reader = list(csv.reader(fd, delimiter=delimiter))
        return self._do_read_metric_xsv(reader, row, col)

    def _read_metric(self, path, typ=None, xpath=None):
        ret = None

        if not os.path.exists(path):
            return ret

        try:
            with open(path, 'r') as fd:
                if typ == 'json':
                    ret = self._read_metric_json(fd, xpath)
                elif typ == 'csv':
                    ret = self._read_metric_xsv(fd, xpath, ',')
                elif typ == 'tsv':
                    ret = self._read_metric_xsv(fd, xpath, '\t')
                elif typ == 'hcsv':
                    ret = self._read_metric_hxsv(fd, xpath, ',')
                elif typ == 'htsv':
                    ret = self._read_metric_hxsv(fd, xpath, '\t')
                else:
                    ret = fd.read()
        except Exception as exc:
            self.logger.error('Unable to read metric in \'{}\''.format(path),
                              exc)

        return ret

    def _find_output_by_path(self, path, outs=None):
        from dvc.exceptions import OutputDuplicationError

        if not outs:
            astages = self.active_stages()
            outs = [out for stage in astages for out in stage.outs]

        abs_path = os.path.abspath(path)
        matched = [out for out in outs if out.path == abs_path]
        stages = [out.stage.path for out in matched]
        if len(stages) > 1:
            raise OutputDuplicationError(path, stages)

        return matched[0] if matched else None

    def metrics_show(self,
                     path=None,
                     typ=None,
                     xpath=None,
                     all_branches=False):
        res = {}
        for branch in self.scm.brancher(all_branches=all_branches):
            astages = self.active_stages()
            outs = [out for stage in astages for out in stage.outs]

            if path:
                out = self._find_output_by_path(path, outs=outs)
                stage = out.stage.path if out else None
                if out and all([out.metric,
                                not typ,
                                isinstance(out.metric, dict)]):
                    entries = [(path,
                                out.metric.get(out.PARAM_METRIC_TYPE, None),
                                out.metric.get(out.PARAM_METRIC_XPATH, None))]
                else:
                    entries = [(path, typ, xpath)]
            else:
                metrics = filter(lambda o: o.metric, outs)
                stage = None
                entries = []
                for o in metrics:
                    if not typ and isinstance(o.metric, dict):
                        t = o.metric.get(o.PARAM_METRIC_TYPE, None)
                        x = o.metric.get(o.PARAM_METRIC_XPATH, None)
                    else:
                        t = typ
                        x = xpath
                    entries.append((o.path, t, x))

            for fname, t, x in entries:
                if stage:
                    self.checkout(stage)

                rel = os.path.relpath(fname)
                metric = self._read_metric(fname,
                                           typ=t,
                                           xpath=x)
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

        if res:
            return res

        if path:
            msg = 'File \'{}\' does not exist'.format(path)
        else:
            msg = 'No metric files in this repository. ' \
                  'Use \'dvc metrics add\' to add a metric file to track.'
        raise DvcException(msg)

    def _metrics_modify(self, path, typ=None, xpath=None, delete=False):
        out = self._find_output_by_path(path)
        if not out:
            msg = 'Unable to find file \'{}\' in the pipeline'.format(path)
            raise DvcException(msg)

        if out.path_info['scheme'] != 'local':
            msg = 'Output \'{}\' scheme \'{}\' is not supported for metrics'
            raise DvcException(msg.format(out.path, out.path_info['scheme']))

        if out.use_cache:
            msg = 'Cached output \'{}\' is not supported for metrics'
            raise DvcException(msg.format(out.rel_path))

        if typ:
            if not isinstance(out.metric, dict):
                out.metric = {}
            out.metric[out.PARAM_METRIC_TYPE] = typ

        if xpath:
            if not isinstance(out.metric, dict):
                out.metric = {}
            out.metric[out.PARAM_METRIC_XPATH] = xpath

        if delete:
            out.metric = None

        out._verify_metric()

        out.stage.dump()

    def metrics_modify(self, path=None, typ=None, xpath=None):
        self._metrics_modify(path, typ, xpath)

    def metrics_add(self, path, typ=None, xpath=None):
        if not typ:
            typ = 'raw'
        self._metrics_modify(path, typ, xpath)

    def metrics_remove(self, path):
        self._metrics_modify(path, delete=True)

    def graph(self):
        import networkx as nx

        G = nx.DiGraph()
        G_active = nx.DiGraph()
        stages = self.stages()

        outs = []
        for stage in stages:
            outs += stage.outs

        # collect the whole DAG
        for stage in stages:
            node = os.path.relpath(stage.path, self.root_dir)

            G.add_node(node, stage=stage)
            G_active.add_node(node, stage=stage)

            for dep in stage.deps:
                for out in outs:
                    if out.path != dep.path \
                       and not dep.path.startswith(out.path + out.sep):
                        continue

                    dep_stage = out.stage
                    dep_node = os.path.relpath(dep_stage.path, self.root_dir)
                    G.add_node(dep_node, stage=dep_stage)
                    G.add_edge(node, dep_node)
                    if not stage.locked:
                        G_active.add_node(dep_node, stage=dep_stage)
                        G_active.add_edge(node, dep_node)

        return G, G_active

    def pipelines(self):
        import networkx as nx

        G, G_active = self.graph()

        if len(G.nodes()) == 0:
            return []

        # find pipeline ends aka "output stages"
        ends = [node for node, in_degree in G.in_degree() if in_degree == 0]

        # filter out subgraphs that didn't exist in original G
        pipelines = []
        for c in nx.weakly_connected_components(G_active):
            H = G_active.subgraph(c)
            found = False
            for node in ends:
                if node in H:
                    found = True
                    break
            if found:
                pipelines.append(H)

        return pipelines

    def stages(self):
        stages = []
        for root, dirs, files in os.walk(self.root_dir):
            for fname in files:
                path = os.path.join(root, fname)
                if not Stage.is_stage_file(path):
                    continue
                stages.append(Stage.load(self, path))
        return stages

    def active_stages(self):
        import networkx as nx

        stages = []
        for G in self.pipelines():
            stages.extend(list(nx.get_node_attributes(G, 'stage').values()))
        return stages
