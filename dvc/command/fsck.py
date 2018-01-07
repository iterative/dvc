import os

from dvc.command.common.base import CmdBase
from dvc.data_cloud import file_md5


class CmdFsck(CmdBase):
    def run(self):
        directions = self.all_directions()
        files_and_stages = self.directions_by_datafile(directions)

        if self.args.targets:
            dvc_files = [os.path.relpath(os.path.abspath(f), self.project.root_dir)
                         for f in self.args.targets]
        else:
            dvc_files = files_and_stages.keys()

        caches = self.project.cache.find_cache(dvc_files)

        self.print_fsck(caches, dvc_files, files_and_stages)
        return 0

    def all_directions(self):
        result = []
        for stage in self.project.stages():
            for dep in stage.deps + stage.outs:
                result.append((stage, dep))
        return result

    def print_fsck(self, caches, dvc_files, files_and_stages):
        for file in dvc_files:
            print(u'File {}:'.format(file))

            full_path = os.path.join(self.project.root_dir, file)
            if self.args.physical:
                md5 = file_md5(full_path)[0]
                print(u'    Actual checksum:        {}'.format(md5))
            else:
                md5 = None

            hardlink_md5 = caches.get(file)
            hardlink_msg = hardlink_md5 if hardlink_md5 else 'No cache file found'
            hardlink_error = '!!!' if hardlink_md5 and md5 and md5 != hardlink_md5 else ''
            print(u'    Hardlink to cache file: {} {}'.format(hardlink_msg, hardlink_error))

            state = self.project.state.get(file)
            local_state_error = '!!!' if state.md5 and hardlink_md5 and state.md5 != hardlink_md5 else ''
            print(u'    Local state checksum:   {} {}'.format(state.md5 if state else '', local_state_error))
            print(u'    Local state mtime:      {}'.format(state.mtime if state else ''))
            mtime = os.path.getmtime(full_path) if os.path.exists(full_path) else 'None'
            print(u'    Actual mtime:           {}'.format(mtime))

            for stage, dep in files_and_stages.get(file, []):
                stage_error = '!!!' if dep.md5 and hardlink_md5 and dep.md5 != hardlink_md5 else ''
                print(u'    Stage file: {}'.format(stage.dvc_path))
                print(u'        Type:               {}'.format(type(dep).__name__))
                print(u'        Checksum:           {} {}'.format(dep.md5, stage_error))
                print(u'        Use cache:          {}'.format(str(dep.use_cache).lower()))
        pass

    @staticmethod
    def directions_by_datafile(directions):
        result = {}

        for stage, dep in directions:
            if dep.dvc_path not in result:
                result[dep.dvc_path] = []
            result[dep.dvc_path].append((stage, dep))

        return result
