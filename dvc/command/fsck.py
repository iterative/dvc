import os

from dvc import stage
from dvc.command.common.base import CmdBase
from dvc.data_cloud import file_md5


class FsckFileDep(object):
    def __init__(self, dvc_file_name, dep):
        self.dvc_file_name = dvc_file_name
        self.md5 = dep.md5
        self.type_name = type(dep).__name__
        self.use_cache = dep.use_cache if hasattr(dep, 'use_cache') else False

    @property
    def is_output(self):
        return self.type_name == stage.Output.__name__


class FsckFile(object):
    ERR_STATUS_NO_CACHE_FILE = 'No cache file found'
    ERR_STATUS_CHECKSUM_MISMATCH = 'Checksum missmatch'

    def __init__(self, dvc_path, full_path, md5, hardlink_md5, state, fsck_deps):
        self.dvc_path = dvc_path
        self.full_path = full_path
        self.md5 = md5
        self.hardlink_md5 = hardlink_md5
        self.state = state
        self.fsck_deps = fsck_deps
        self.mtime = os.path.getmtime(full_path) if os.path.exists(full_path) else None

    def hardlink_msg(self):
        if not self.is_data_file():
            return 'Not data file -  no cache needed'
        elif self.hardlink_md5:
            return self.hardlink_md5
        return 'none'

    def local_state_checksum_msg(self):
        return u'{}'.format(self.state.md5 if self.state else 'none')

    def state_mtime_msg(self):
        return self.state.mtime if self.state else 'none'

    def mtime_msg(self):
        return os.path.getmtime(self.full_path) if os.path.exists(self.full_path) else 'none'

    def is_data_file(self):
        return any([d.is_output and d.use_cache for d in self.fsck_deps])

    def checksums_mismatch(self):
        md5_set = {self.md5, self.hardlink_md5}
        md5_deps = set([d.md5 for d in self.fsck_deps])
        md5_set = md5_set.union(md5_deps)
        if None in md5_set:
            md5_set.remove(None)
        return len(md5_set) != 1

    @property
    def error_status(self):
        if self.checksums_mismatch():
            return self.ERR_STATUS_CHECKSUM_MISMATCH
        if self.is_data_file() and self.hardlink_md5 is None:
            return self.ERR_STATUS_NO_CACHE_FILE
        return None

    def print_info(self):
        print(u'File {}:'.format(self.dvc_path))

        if self.error_status:
            print(u'    Error status:           {}!!!'.format(self.error_status))

        if self.md5:
            print(u'    Actual checksum:        {}'.format(self.md5))

        print(u'    Cache file name:        {}'.format(self.hardlink_msg()))
        print(u'    Local state checksum:   {}'.format(self.local_state_checksum_msg()))
        print(u'    Local state mtime:      {}'.format(self.state_mtime_msg()))
        print(u'    Actual mtime:           {}'.format(self.mtime_msg()))

        for fsck_deps in self.fsck_deps:
            print(u'    Stage file: {}'.format(fsck_deps.dvc_file_name))
            print(u'        Checksum:           {}'.format(fsck_deps.md5))
            print(u'        Type:               {}'.format(fsck_deps.type_name))
            if fsck_deps.use_cache:
                print(u'        Use cache:          {}'.format(str(fsck_deps.use_cache).lower()))
        pass


class Fsck(object):
    def __init__(self, project, targets=None, physical=False, all=False):
        self.project = project
        self.targets = targets or []
        self.physical = physical
        self.all = all

        self.files_and_stages = self.directions_by_datafile(self.all_directions())
        self.caches = self.project.cache.find_cache(self.files_and_stages)

        self.fsck_objs = self.dvc_files_to_fsck_objs()

    @property
    def is_target_defined(self):
        return len(self.targets) > 0

    def file_list(self, files_and_stages, targets):
        if targets:
            dvc_files = [os.path.relpath(os.path.abspath(f), self.project.root_dir)
                         for f in targets]
        else:
            dvc_files = files_and_stages.keys()
        return dvc_files

    def all_directions(self):
        result = []
        for stage in self.project.stages():
            for dep in stage.deps + stage.outs:
                result.append((stage, dep))
        return result

    def create_file_fsck_state(self, dvc_path):
        full_path = os.path.join(self.project.root_dir, dvc_path)

        if self.physical:
            md5 = file_md5(full_path)[0]
        else:
            md5 = None

        hardlink_md5 = self.caches.get(dvc_path)
        state = self.project.state.get(dvc_path)
        fsck_deps = [FsckFileDep(stage.dvc_path, dep)
                     for stage, dep in self.files_and_stages.get(dvc_path, [])]

        return FsckFile(dvc_path, full_path, md5, hardlink_md5, state, fsck_deps)

    def dvc_files_to_fsck_objs(self):
        if not self.is_target_defined or self.all:
            files = self.files_and_stages.keys()
        else:
            files = map(lambda t: os.path.relpath(os.path.abspath(t), self.project.root_dir),
                        self.targets)

        fsck_objs = map(lambda file: self.create_file_fsck_state(file), files)

        if not self.is_target_defined and not self.all:
            fsck_objs = filter(lambda o: o.error_status is not None, fsck_objs)

        return list(fsck_objs)

    @staticmethod
    def directions_by_datafile(directions):
        result = {}

        for stage, dep in directions:
            if dep.dvc_path not in result:
                result[dep.dvc_path] = []
            result[dep.dvc_path].append((stage, dep))

        return result


class CmdFsck(CmdBase):
    def run(self):
        return self.fsck(self.args.targets, self.args.physical, self.args.all)

    def fsck(self, targets, physical, all):
        checker = Fsck(self.project, targets, physical, all)

        for file_fsck in checker.fsck_objs:
            file_fsck.print_info()
        return 0
