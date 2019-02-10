from __future__ import unicode_literals

from dvc.utils.compat import str, builtin_str, open

import collections
import os
import dvc.prompt as prompt
import dvc.logger as logger

from dvc.exceptions import (
    DvcException,
    MoveNotDataSourceError,
    NotDvcProjectError,
)


class InitError(DvcException):
    def __init__(self, msg):
        super(InitError, self).__init__(msg)


class ReproductionError(DvcException):
    def __init__(self, dvc_file_name, ex):
        self.path = dvc_file_name
        msg = "failed to reproduce '{}'".format(dvc_file_name)
        super(ReproductionError, self).__init__(msg, cause=ex)


class Project(object):
    DVC_DIR = ".dvc"

    def __init__(self, root_dir=None):
        from dvc.config import Config
        from dvc.state import State
        from dvc.lock import Lock
        from dvc.scm import SCM
        from dvc.cache import Cache
        from dvc.data_cloud import DataCloud
        from dvc.updater import Updater

        root_dir = self.find_root(root_dir)

        self.root_dir = os.path.abspath(os.path.realpath(root_dir))
        self.dvc_dir = os.path.join(self.root_dir, self.DVC_DIR)

        self.config = Config(self.dvc_dir)

        self.scm = SCM(self.root_dir, project=self)
        self.lock = Lock(self.dvc_dir)
        # NOTE: storing state and link_state in the repository itself to avoid
        # any possible state corruption in 'shared cache dir' scenario.
        self.state = State(self, self.config.config)

        core = self.config.config[Config.SECTION_CORE]

        logger.set_level(core.get(Config.SECTION_CORE_LOGLEVEL))

        self.cache = Cache(self)
        self.cloud = DataCloud(self, config=self.config.config)
        self.updater = Updater(self.dvc_dir)

        self.files_to_git_add = []

        self._ignore()

        self.updater.check()

    def __repr__(self):
        return "Project: '{root_dir}'".format(root_dir=self.root_dir)

    @staticmethod
    def find_root(root=None):
        if root is None:
            root = os.getcwd()
        else:
            root = os.path.abspath(os.path.realpath(root))

        while True:
            dvc_dir = os.path.join(root, Project.DVC_DIR)
            if os.path.isdir(dvc_dir):
                return root
            if os.path.ismount(root):
                break
            root = os.path.dirname(root)
        raise NotDvcProjectError(root)

    @staticmethod
    def find_dvc_dir(root=None):
        root_dir = Project.find_root(root)
        return os.path.join(root_dir, Project.DVC_DIR)

    def _remind_to_git_add(self):
        if not self.files_to_git_add:
            return

        logger.info(
            "\n"
            "To track the changes with git run:\n"
            "\n"
            "\tgit add {files}".format(files=" ".join(self.files_to_git_add))
        )

    @staticmethod
    def init(root_dir=os.curdir, no_scm=False, force=False):
        """
        Creates an empty project on the given directory -- basically a
        `.dvc` directory with subdirectories for configuration and cache.

        It should be tracked by a SCM or use the `--no-scm` flag.

        If the given directory is not empty, you must use the `--force`
        flag to override it.

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
            raise InitError(
                "{project} is not tracked by any supported scm tool"
                " (e.g. git). Use '--no-scm' if you don't want to use any scm.".format(
                    project=root_dir
                )
            )

        if os.path.isdir(dvc_dir):
            if not force:
                raise InitError(
                    "'{project}' exists. Use '-f' to force.".format(
                        project=os.path.relpath(dvc_dir)
                    )
                )

            shutil.rmtree(dvc_dir)

        os.mkdir(dvc_dir)

        config = Config.init(dvc_dir)
        proj = Project(root_dir)

        scm.add([config.config_file])

        if scm.ignore_file:
            scm.add([os.path.join(dvc_dir, scm.ignore_file)])
            logger.info("\nYou can now commit the changes to git.\n")

        proj._welcome_message()

        return proj

    def destroy(self):
        import shutil

        for stage in self.stages():
            stage.remove()

        shutil.rmtree(self.dvc_dir)

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

    def install(self):
        self.scm.install()

    @staticmethod
    def _check_cyclic_graph(graph):
        import networkx as nx
        from dvc.exceptions import CyclicGraphError

        cycles = list(nx.simple_cycles(graph))

        if cycles:
            raise CyclicGraphError(cycles[0])

    def add(self, fname, recursive=False):
        from dvc.stage import Stage

        fnames = []
        if recursive and os.path.isdir(fname):
            for root, dirs, files in os.walk(str(fname)):
                for f in files:
                    path = os.path.join(root, f)
                    if Stage.is_stage_file(path):
                        continue
                    if os.path.basename(path) == self.scm.ignore_file:
                        continue
                    if self.scm.is_tracked(path):
                        continue
                    fnames.append(path)
        else:
            fnames = [fname]

        stages = []
        self.files_to_git_add = []
        with self.state:
            for f in fnames:
                stage = Stage.create(project=self, outs=[f], add=True)

                if stage is None:
                    stages.append(stage)
                    continue

                stage.save()
                stages.append(stage)

        self._check_dag(self.stages() + stages)

        for stage in stages:
            if stage is not None:
                stage.dump()

        self._remind_to_git_add()

        return stages

    def _check_dag(self, stages):
        """Generate graph including the new stage to check for errors"""
        self.graph(stages=stages)

    def remove(self, target, outs_only=False):
        from dvc.stage import Stage

        stage = Stage.load(self, target)
        if outs_only:
            stage.remove_outs()
        else:
            stage.remove()

        return stage

    def lock_stage(self, target, unlock=False):
        from dvc.stage import Stage

        stage = Stage.load(self, target)
        stage.locked = False if unlock else True
        stage.dump()

        return stage

    def move(self, from_path, to_path):
        """
        Renames an output file and modifies the stage associated
        to reflect the change on the pipeline.

        If the output has the same name as its stage, it would
        also rename the corresponding stage file.

        E.g.
              Having: (hello, hello.dvc)

              $ dvc move hello greetings

              Result: (greeting, greeting.dvc)

        It only works with outputs generated by `add` or `import`,
        also known as data sources.
        """
        import dvc.output as Output
        from dvc.stage import Stage

        from_out = Output.loads_from(Stage(self, cwd=os.curdir), [from_path])[
            0
        ]

        to_path = self._expand_target_path(from_path, to_path)

        try:
            stage, out = next(
                (stage, out)
                for stage in self.stages()
                for out in stage.outs
                if from_out.path == out.path
            )
        except StopIteration:
            raise DvcException(
                "unable to find stage file with output '{path}'".format(
                    path=from_path
                )
            )

        if not stage.is_data_source:
            raise MoveNotDataSourceError(stage.relpath)

        stage_name = os.path.splitext(os.path.basename(stage.path))[0]
        from_name = os.path.basename(from_out.path)
        if stage_name == from_name:
            os.unlink(stage.path)

            stage.path = os.path.join(
                os.path.dirname(to_path),
                os.path.basename(to_path) + Stage.STAGE_FILE_SUFFIX,
            )

            stage.cwd = os.path.join(self.root_dir, os.path.dirname(to_path))

        to_out = Output.loads_from(
            stage, [os.path.basename(to_path)], out.cache, out.metric
        )[0]

        with self.state:
            out.move(to_out)

        stage.dump()

        self._remind_to_git_add()

    def _unprotect_file(self, path):
        import stat
        import uuid
        from dvc.system import System
        from dvc.utils import copyfile, move, remove

        if System.is_symlink(path) or System.is_hardlink(path):
            logger.debug("Unprotecting '{}'".format(path))

            tmp = os.path.join(os.path.dirname(path), "." + str(uuid.uuid4()))
            move(path, tmp)

            copyfile(tmp, path)

            remove(tmp)
        else:
            logger.debug(
                "Skipping copying for '{}', since it is not "
                "a symlink or a hardlink.".format(path)
            )

        os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)

    def _unprotect_dir(self, path):
        for root, dirs, files in os.walk(str(path)):
            for f in files:
                path = os.path.join(root, f)
                self._unprotect_file(path)

    def unprotect(self, path):
        if not os.path.exists(path):
            raise DvcException(
                "can't unprotect non-existing data '{}'".format(path)
            )

        if os.path.isdir(path):
            self._unprotect_dir(path)
        else:
            self._unprotect_file(path)

    def run(
        self,
        cmd=None,
        deps=None,
        outs=None,
        outs_no_cache=None,
        metrics=None,
        metrics_no_cache=None,
        fname=None,
        cwd=os.curdir,
        no_exec=False,
        overwrite=False,
        ignore_build_cache=False,
        remove_outs=False,
    ):
        from dvc.stage import Stage

        if outs is None:
            outs = []
        if deps is None:
            deps = []
        if outs_no_cache is None:
            outs_no_cache = []
        if metrics is None:
            metrics = []
        if metrics_no_cache is None:
            metrics_no_cache = []

        with self.state:
            stage = Stage.create(
                project=self,
                fname=fname,
                cmd=cmd,
                cwd=cwd,
                outs=outs,
                outs_no_cache=outs_no_cache,
                metrics=metrics,
                metrics_no_cache=metrics_no_cache,
                deps=deps,
                overwrite=overwrite,
                ignore_build_cache=ignore_build_cache,
                remove_outs=remove_outs,
            )

        if stage is None:
            return None

        self._check_dag(self.stages() + [stage])

        self.files_to_git_add = []
        with self.state:
            if not no_exec:
                stage.run()

        stage.dump()

        self._remind_to_git_add()

        return stage

    def imp(self, url, out, resume=False):
        from dvc.stage import Stage

        stage = Stage.create(project=self, cmd=None, deps=[url], outs=[out])

        if stage is None:
            return None

        self._check_dag(self.stages() + [stage])

        self.files_to_git_add = []
        with self.state:
            stage.run(resume=resume)

        stage.dump()

        self._remind_to_git_add()

        return stage

    def _reproduce_stage(self, stages, node, force, dry, interactive):
        stage = stages[node]

        if stage.locked:
            logger.warning(
                "DVC file '{path}' is locked. Its dependencies are"
                " not going to be reproduced.".format(path=stage.relpath)
            )

        stage = stage.reproduce(force=force, dry=dry, interactive=interactive)
        if not stage:
            return []

        if not dry:
            stage.dump()

        return [stage]

    def reproduce(
        self,
        target=None,
        recursive=True,
        force=False,
        dry=False,
        interactive=False,
        pipeline=False,
        all_pipelines=False,
        ignore_build_cache=False,
    ):
        from dvc.stage import Stage

        if not target and not all_pipelines:
            raise ValueError()

        if not interactive:
            config = self.config
            core = config.config[config.SECTION_CORE]
            interactive = core.get(config.SECTION_CORE_INTERACTIVE, False)

        targets = []
        if pipeline or all_pipelines:
            if pipeline:
                stage = Stage.load(self, target)
                node = os.path.relpath(stage.path, self.root_dir)
                pipelines = [self._get_pipeline(node)]
            else:
                pipelines = self.pipelines()

            for G in pipelines:
                for node in G.nodes():
                    if G.in_degree(node) == 0:
                        targets.append(os.path.join(self.root_dir, node))
        else:
            targets.append(target)

        self.files_to_git_add = []

        ret = []
        with self.state:
            for target in targets:
                stages = self._reproduce(
                    target,
                    recursive=recursive,
                    force=force,
                    dry=dry,
                    interactive=interactive,
                    ignore_build_cache=ignore_build_cache,
                )
                ret.extend(stages)

        self._remind_to_git_add()

        return ret

    def _reproduce(
        self,
        target,
        recursive=True,
        force=False,
        dry=False,
        interactive=False,
        ignore_build_cache=False,
    ):
        import networkx as nx
        from dvc.stage import Stage

        stage = Stage.load(self, target)
        G = self.graph()[1]
        stages = nx.get_node_attributes(G, "stage")
        node = os.path.relpath(stage.path, self.root_dir)

        if recursive:
            ret = self._reproduce_stages(
                G, stages, node, force, dry, interactive, ignore_build_cache
            )
        else:
            ret = self._reproduce_stage(stages, node, force, dry, interactive)

        return ret

    def _reproduce_stages(
        self, G, stages, node, force, dry, interactive, ignore_build_cache
    ):
        import networkx as nx

        result = []
        for n in nx.dfs_postorder_nodes(G, node):
            try:
                ret = self._reproduce_stage(stages, n, force, dry, interactive)

                if len(ret) == 0 and ignore_build_cache:
                    # NOTE: we are walking our pipeline from the top to the
                    # bottom. If one stage is changed, it will be reproduced,
                    # which tells us that we should force reproducing all of
                    # the other stages down below, even if their direct
                    # dependencies didn't change.
                    force = True

                result += ret
            except Exception as ex:
                raise ReproductionError(stages[n].relpath, ex)
        return result

    def _cleanup_unused_links(self, all_stages):
        used = []
        for stage in all_stages:
            for out in stage.outs:
                used.append(out.path)
        self.state.remove_unused_links(used)

    def checkout(
        self, target=None, with_deps=False, force=False, recursive=False
    ):
        if target and not recursive:
            from dvc.stage import (
                StageFileDoesNotExistError,
                StageFileBadNameError,
            )

            all_stages = self.active_stages()
            try:
                stages = self._collect(target, with_deps=with_deps)
            except (StageFileDoesNotExistError, StageFileBadNameError) as exc:
                raise DvcException(
                    str(exc)
                    + " Did you mean 'git checkout {}'?".format(target)
                )
        else:
            all_stages = self.active_stages(target)
            stages = all_stages

        with self.state:
            self._cleanup_unused_links(all_stages)

            for stage in stages:
                if stage.locked:
                    logger.warning(
                        "DVC file '{path}' is locked. Its dependencies are"
                        " not going to be checked out.".format(
                            path=stage.relpath
                        )
                    )

                stage.checkout(force=force)

    def _get_pipeline(self, node):
        pipelines = list(filter(lambda g: node in g.nodes(), self.pipelines()))
        assert len(pipelines) == 1
        return pipelines[0]

    def _collect(self, target, with_deps=False):
        import networkx as nx
        from dvc.stage import Stage

        stage = Stage.load(self, target)
        if not with_deps:
            return [stage]

        node = os.path.relpath(stage.path, self.root_dir)
        G = self._get_pipeline(node)
        stages = nx.get_node_attributes(G, "stage")

        ret = [stage]
        for n in nx.dfs_postorder_nodes(G, node):
            ret.append(stages[n])

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

        for i in self.cache.local.load_dir_cache(md5):
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

        md5 = info[out.remote.PARAM_CHECKSUM]
        cache = self.cache.local.get(md5)
        if not out.remote.is_dir_cache(cache):
            return ret

        return self._collect_dir_cache(
            out, branch=branch, remote=remote, force=force, jobs=jobs
        )

    def _used_cache(
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

        for branch in self.scm.brancher(
            all_branches=all_branches, all_tags=all_tags
        ):
            if target:
                if recursive:
                    stages = self.stages(target)
                else:
                    stages = self._collect(target, with_deps=with_deps)
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
                    scheme = out.path_info["scheme"]
                    cache[scheme] += self._collect_used_cache(
                        out,
                        branch=branch,
                        remote=remote,
                        force=force,
                        jobs=jobs,
                    )

        return cache

    @staticmethod
    def merge_cache_lists(clists):
        merged_cache = collections.defaultdict(list)

        for cache_list in clists:
            for scheme, cache in cache_list.items():
                for item in cache:
                    if item not in merged_cache[scheme]:
                        merged_cache[scheme].append(item)

        return merged_cache

    @staticmethod
    def load_all_used_cache(
        projects,
        target=None,
        all_branches=False,
        active=True,
        with_deps=False,
        all_tags=False,
        remote=None,
        force=False,
        jobs=None,
    ):
        clists = []

        for project in projects:
            with project.state:
                project_clist = project._used_cache(
                    target=None,
                    all_branches=all_branches,
                    active=False,
                    with_deps=with_deps,
                    all_tags=all_tags,
                    remote=remote,
                    force=force,
                    jobs=jobs,
                )

                clists.append(project_clist)

        return clists

    def _do_gc(self, typ, func, clist):
        removed = func(clist)
        if not removed:
            logger.info("No unused {} cache to remove.".format(typ))

    def gc(
        self,
        all_branches=False,
        cloud=False,
        remote=None,
        with_deps=False,
        all_tags=False,
        force=False,
        jobs=None,
        projects=None,
    ):

        all_projects = [self]

        if projects:
            all_projects.extend(Project(path) for path in projects)

        all_clists = Project.load_all_used_cache(
            all_projects,
            target=None,
            all_branches=all_branches,
            active=False,
            with_deps=with_deps,
            all_tags=all_tags,
            remote=remote,
            force=force,
            jobs=jobs,
        )

        if len(all_clists) > 1:
            clist = Project.merge_cache_lists(all_clists)
        else:
            clist = all_clists[0]

        with self.state:
            self._do_gc("local", self.cache.local.gc, clist)

            if self.cache.s3:
                self._do_gc("s3", self.cache.s3.gc, clist)

            if self.cache.gs:
                self._do_gc("gs", self.cache.gs.gc, clist)

            if self.cache.ssh:
                self._do_gc("ssh", self.cache.ssh.gc, clist)

            if self.cache.hdfs:
                self._do_gc("hdfs", self.cache.hdfs.gc, clist)

            if self.cache.azure:
                self._do_gc("azure", self.cache.azure.gc, clist)

            if cloud:
                self._do_gc(
                    "remote", self.cloud._get_cloud(remote, "gc -c").gc, clist
                )

    def push(
        self,
        target=None,
        jobs=1,
        remote=None,
        all_branches=False,
        show_checksums=False,
        with_deps=False,
        all_tags=False,
        recursive=False,
    ):
        with self.state:
            used = self._used_cache(
                target,
                all_branches=all_branches,
                all_tags=all_tags,
                with_deps=with_deps,
                force=True,
                remote=remote,
                jobs=jobs,
                recursive=recursive,
            )["local"]
            self.cloud.push(
                used, jobs, remote=remote, show_checksums=show_checksums
            )

    def fetch(
        self,
        target=None,
        jobs=1,
        remote=None,
        all_branches=False,
        show_checksums=False,
        with_deps=False,
        all_tags=False,
        recursive=False,
    ):
        with self.state:
            used = self._used_cache(
                target,
                all_branches=all_branches,
                all_tags=all_tags,
                with_deps=with_deps,
                force=True,
                remote=remote,
                jobs=jobs,
                recursive=recursive,
            )["local"]
            self.cloud.pull(
                used, jobs, remote=remote, show_checksums=show_checksums
            )

    def pull(
        self,
        target=None,
        jobs=1,
        remote=None,
        all_branches=False,
        show_checksums=False,
        with_deps=False,
        all_tags=False,
        force=False,
        recursive=False,
    ):
        self.fetch(
            target,
            jobs,
            remote=remote,
            all_branches=all_branches,
            all_tags=all_tags,
            show_checksums=show_checksums,
            with_deps=with_deps,
            recursive=recursive,
        )
        self.checkout(
            target=target,
            with_deps=with_deps,
            force=force,
            recursive=recursive,
        )

    def _local_status(self, target=None, with_deps=False):
        status = {}

        if target:
            stages = self._collect(target, with_deps=with_deps)
        else:
            stages = self.active_stages()

        for stage in stages:
            if stage.locked:
                logger.warning(
                    "DVC file '{path}' is locked. Its dependencies are"
                    " not going to be shown in the status output.".format(
                        path=stage.relpath
                    )
                )

            status.update(stage.status())

        return status

    def _cloud_status(
        self,
        target=None,
        jobs=1,
        remote=None,
        show_checksums=False,
        all_branches=False,
        with_deps=False,
        all_tags=False,
    ):
        import dvc.remote.base as cloud

        used = self._used_cache(
            target,
            all_branches=all_branches,
            all_tags=all_tags,
            with_deps=with_deps,
            force=True,
            remote=remote,
            jobs=jobs,
        )["local"]

        ret = {}
        status_info = self.cloud.status(
            used, jobs, remote=remote, show_checksums=show_checksums
        )
        for md5, info in status_info.items():
            name = info["name"]
            status = info["status"]
            if status == cloud.STATUS_OK:
                continue

            prefix_map = {
                cloud.STATUS_DELETED: "deleted",
                cloud.STATUS_NEW: "new",
            }

            ret[name] = prefix_map[status]

        return ret

    def status(
        self,
        target=None,
        jobs=1,
        cloud=False,
        remote=None,
        show_checksums=False,
        all_branches=False,
        with_deps=False,
        all_tags=False,
    ):
        with self.state:
            if cloud:
                return self._cloud_status(
                    target,
                    jobs,
                    remote=remote,
                    show_checksums=show_checksums,
                    all_branches=all_branches,
                    with_deps=with_deps,
                    all_tags=all_tags,
                )
            return self._local_status(target, with_deps=with_deps)

    @staticmethod
    def _read_metric_json(fd, json_path):
        import json
        from jsonpath_rw import parse

        parser = parse(json_path)
        return [x.value for x in parser.find(json.load(fd))]

    @staticmethod
    def _do_read_metric_xsv(reader, row, col):
        if col is not None and row is not None:
            return [reader[row][col]]
        elif col is not None:
            return [r[col] for r in reader]
        elif row is not None:
            return reader[row]
        return None

    @classmethod
    def _read_metric_hxsv(cls, fd, hxsv_path, delimiter):
        import csv

        col, row = hxsv_path.split(",")
        row = int(row)
        reader = list(csv.DictReader(fd, delimiter=builtin_str(delimiter)))
        return cls._do_read_metric_xsv(reader, row, col)

    @classmethod
    def _read_metric_xsv(cls, fd, xsv_path, delimiter):
        import csv

        col, row = xsv_path.split(",")
        row = int(row)
        col = int(col)
        reader = list(csv.reader(fd, delimiter=builtin_str(delimiter)))
        return cls._do_read_metric_xsv(reader, row, col)

    def _read_metric(self, path, typ=None, xpath=None):
        ret = None

        if not os.path.exists(path):
            return ret

        try:
            with open(path, "r") as fd:
                if typ == "json":
                    ret = self._read_metric_json(fd, xpath)
                elif typ == "csv":
                    ret = self._read_metric_xsv(fd, xpath, ",")
                elif typ == "tsv":
                    ret = self._read_metric_xsv(fd, xpath, "\t")
                elif typ == "hcsv":
                    ret = self._read_metric_hxsv(fd, xpath, ",")
                elif typ == "htsv":
                    ret = self._read_metric_hxsv(fd, xpath, "\t")
                else:
                    ret = fd.read().strip()
        except Exception:
            logger.error("unable to read metric in '{}'".format(path))

        return ret

    def _find_output_by_path(self, path, outs=None, recursive=False):
        from dvc.exceptions import OutputDuplicationError

        if not outs:
            astages = self.active_stages()
            outs = [out for stage in astages for out in stage.outs]

        abs_path = os.path.abspath(path)
        if os.path.isdir(abs_path) and recursive:
            matched = [
                out
                for out in outs
                if os.path.abspath(out.path).startswith(abs_path)
            ]
        else:
            matched = [out for out in outs if out.path == abs_path]
            stages = [out.stage.relpath for out in matched]
            if len(stages) > 1:
                raise OutputDuplicationError(path, stages)

        return matched if matched else []

    def metrics_show(
        self,
        path=None,
        typ=None,
        xpath=None,
        all_branches=False,
        all_tags=False,
        recursive=False,
    ):
        res = {}
        for branch in self.scm.brancher(
            all_branches=all_branches, all_tags=all_tags
        ):

            astages = self.active_stages()
            outs = [out for stage in astages for out in stage.outs]

            if path:
                outs = self._find_output_by_path(
                    path, outs=outs, recursive=recursive
                )
                stages = [out.stage.path for out in outs]
                entries = []
                for out in outs:
                    if all(
                        [out.metric, not typ, isinstance(out.metric, dict)]
                    ):
                        entries += [
                            (
                                out.path,
                                out.metric.get(out.PARAM_METRIC_TYPE, None),
                                out.metric.get(out.PARAM_METRIC_XPATH, None),
                            )
                        ]
                    else:
                        entries += [(out.path, typ, xpath)]
                if not entries:
                    if os.path.isdir(path):
                        logger.warning(
                            "Path '{path}' is a directory. "
                            "Consider running with '-R'.".format(path=path)
                        )
                        return {}

                    else:
                        entries += [(path, typ, xpath)]

            else:
                metrics = filter(lambda o: o.metric, outs)
                stages = None
                entries = []
                for o in metrics:
                    if not typ and isinstance(o.metric, dict):
                        t = o.metric.get(o.PARAM_METRIC_TYPE, typ)
                        x = o.metric.get(o.PARAM_METRIC_XPATH, xpath)
                    else:
                        t = typ
                        x = xpath
                    entries.append((o.path, t, x))

            for fname, t, x in entries:
                if stages:
                    for stage in stages:
                        self.checkout(stage, force=True)

                rel = os.path.relpath(fname)
                metric = self._read_metric(fname, typ=t, xpath=x)
                if not metric:
                    continue

                if branch not in res:
                    res[branch] = {}

                res[branch][rel] = metric

        for branch, val in res.items():
            if all_branches or all_tags:
                logger.info("{}:".format(branch))
            for fname, metric in val.items():
                logger.info("\t{}: {}".format(fname, metric))

        if res:
            return res

        if path and os.path.isdir(path):
            return res

        if path:
            msg = "file '{}' does not exist or malformed".format(path)
        else:
            msg = (
                "no metric files in this repository."
                " use 'dvc metrics add' to add a metric file to track."
            )

        raise DvcException(msg)

    def _metrics_modify(self, path, typ=None, xpath=None, delete=False):
        outs = self._find_output_by_path(path)

        if not outs:
            msg = "unable to find file '{}' in the pipeline".format(path)
            raise DvcException(msg)

        if len(outs) != 1:
            msg = (
                "-R not yet supported for metrics modify. "
                "Make sure only one metric is referred to by '{}'".format(path)
            )
            raise DvcException(msg)

        out = outs[0]

        if out.scheme != "local":
            msg = "output '{}' scheme '{}' is not supported for metrics"
            raise DvcException(msg.format(out.path, out.path_info["scheme"]))

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
            typ = "raw"
        self._metrics_modify(path, typ, xpath)

    def metrics_remove(self, path):
        self._metrics_modify(path, delete=True)

    def graph(self, stages=None, from_directory=None):
        import networkx as nx
        from dvc.exceptions import (
            OutputDuplicationError,
            WorkingDirectoryAsOutputError,
        )

        G = nx.DiGraph()
        G_active = nx.DiGraph()
        stages = stages or self.stages(from_directory)
        stages = [stage for stage in stages if stage]
        outs = []

        for stage in stages:
            for out in stage.outs:
                existing = [o.stage for o in outs if o.path == out.path]

                if existing:
                    stages = [stage.relpath, existing[0].relpath]
                    raise OutputDuplicationError(out.path, stages)

                outs.append(out)

        for stage in stages:
            for out in outs:
                overlaps = stage.cwd == out.path or stage.cwd.startswith(
                    out.path + os.sep
                )

                if overlaps:
                    raise WorkingDirectoryAsOutputError(
                        stage.cwd, stage.relpath
                    )

        # collect the whole DAG
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

    def stages(self, from_directory=None):
        """
        Walks down the root directory looking for Dvcfiles,
        skipping the directories that are related with
        any SCM (e.g. `.git`), DVC itself (`.dvc`), or directories
        tracked by DVC (e.g. `dvc add data` would skip `data/`)

        NOTE: For large projects, this could be an expensive
              operation. Consider using some memoization.
        """
        from dvc.stage import Stage

        if not from_directory:
            from_directory = self.root_dir

        stages = []
        outs = []
        for root, dirs, files in os.walk(str(from_directory)):
            for fname in files:
                path = os.path.join(root, fname)
                if not Stage.is_stage_file(path):
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

        return stages

    def active_stages(self, from_directory=None):
        import networkx as nx

        stages = []
        for G in self.pipelines(from_directory):
            stages.extend(list(nx.get_node_attributes(G, "stage").values()))
        return stages

    @staticmethod
    def _welcome_message():
        import colorama

        logger.box(
            "DVC has enabled anonymous aggregate usage analytics.\n"
            "Read the analytics documentation (and how to opt-out) here:\n"
            "{blue}https://dvc.org/doc/user-guide/analytics{nc}".format(
                blue=colorama.Fore.BLUE, nc=colorama.Fore.RESET
            ),
            border_color="red",
        )

        logger.info(
            "{yellow}What's next?{nc}\n"
            "{yellow}------------{nc}\n"
            "- Check out the documentation: {blue}https://dvc.org/doc{nc}\n"
            "- Get help and share ideas: {blue}https://dvc.org/chat{nc}\n"
            "- Star us on GitHub: {blue}https://github.com/iterative/dvc{nc}".format(
                yellow=colorama.Fore.YELLOW,
                blue=colorama.Fore.BLUE,
                nc=colorama.Fore.RESET,
            )
        )

    @staticmethod
    def _expand_target_path(from_path, to_path):
        if os.path.isdir(to_path) and not os.path.isdir(from_path):
            return os.path.join(to_path, os.path.basename(from_path))
        return to_path
