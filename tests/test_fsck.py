import os

from dvc import cli
from tests.basic_env import TestDvc
from dvc.command.fsck import Fsck, FsckFile, CmdFsck


class TestFsck(TestDvc):
    def no_file_corruptions_test(self):
        fsck_objs = Fsck(self.dvc).fsck_objs
        self.assertEqual(len(fsck_objs), 0)

    def get_all_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)
        fsck_objs = Fsck(self.dvc, all=True).fsck_objs

        self.assertEqual(len(fsck_objs), 2)
        for obj in fsck_objs:
            self.assertIsNone(obj.error_status)

    def get_one_of_many_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)
        fsck_objs = Fsck(self.dvc, [self.FOO]).fsck_objs

        self.assertEqual(len(fsck_objs), 1)
        self.assertEqual(fsck_objs[0].dvc_path, self.FOO)

    def by_file_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)
        fsck_objs = Fsck(self.dvc, [self.FOO, self.BAR]).fsck_objs

        self.assertEqual(len(fsck_objs), 2)
        for obj in fsck_objs:
            self.assertIsNone(obj.error_status)

    def defined_target_with_mismatch_test(self):
        stage_foo = self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        for out in stage_foo.outs:
            if out.dvc_path == self.FOO:
                out.md5 = out.md5 + '1234fe'
        stage_foo.dump()

        fsck_objs = Fsck(self.dvc, [self.FOO, self.BAR]).fsck_objs
        self.assertEqual(len(fsck_objs), 2)

    def checksum_mismatch_test(self):
        stage_foo = self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        for out in stage_foo.outs:
            if out.dvc_path == self.FOO:
                out.md5 = out.md5 + '1234fe'
        stage_foo.dump()

        fsck_objs = Fsck(self.dvc).fsck_objs
        self.assertEqual(len(fsck_objs), 1)
        self.assertEqual(fsck_objs[0].dvc_path, self.FOO)
        self.assertEqual(fsck_objs[0].error_status, FsckFile.ERR_STATUS_CHECKSUM_MISMATCH)

    def no_cache_file_test(self):
        stage_foo = self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        md5 = ''
        for out in stage_foo.outs:
            if out.dvc_path == self.FOO:
                md5 = out.md5

        foo_cache_file = os.path.join(self.dvc.cache.cache_dir, md5)
        self.assertTrue(os.path.exists(foo_cache_file))

        os.chmod(foo_cache_file, 0o777)
        os.remove(foo_cache_file)
        self.assertFalse(os.path.exists(foo_cache_file))

        fsck_objs = Fsck(self.dvc).fsck_objs
        self.assertEqual(len(fsck_objs), 1)
        self.assertEqual(fsck_objs[0].dvc_path, self.FOO)
        self.assertEqual(fsck_objs[0].error_status, FsckFile.ERR_STATUS_NO_CACHE_FILE)

    def physical_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        os.chmod(self.FOO, 0o777)
        os.remove(self.FOO)
        with open(self.FOO, 'w') as fd:
            fd.write('randome stuff s9dfj')

        fsck_objs = Fsck(self.dvc, physical=True).fsck_objs

        self.assertEqual(len(fsck_objs), 1)
        self.assertEqual(fsck_objs[0].dvc_path, self.FOO)

    def by_timestemp_physical_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        os.chmod(self.FOO, 0o777)
        os.remove(self.FOO)
        with open(self.FOO, 'w') as fd:
            fd.write('randome stuff 39re2fwecsb nj')

        fsck_objs = Fsck(self.dvc, physical=False).fsck_objs

        self.assertEqual(len(fsck_objs), 1)
        self.assertEqual(fsck_objs[0].dvc_path, self.FOO)

    def end_to_end_test(self):
        self.dvc.add(self.FOO)
        self.dvc.add(self.BAR)

        os.chmod(self.FOO, 0o777)
        os.remove(self.FOO)

        no_data_file = 'baz'
        open(no_data_file, 'w').write('asd')

        with open(self.FOO, 'w') as fd:
            fd.write('randome stuff s9dfj')

        parser = cli.parse_args(['fsck', self.FOO, self.BAR, no_data_file])
        cmd = CmdFsck(args=parser)
        cmd.run()
