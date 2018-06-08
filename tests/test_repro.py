import os
import yaml
import stat
import shutil
import filecmp

import boto3
import uuid
from google.cloud import storage as gc

from dvc.main import main
from dvc.command.repro import CmdRepro
from dvc.project import Project, ReproductionError
from dvc.utils import file_md5
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage

from tests.basic_env import TestDvc
from tests.test_data_cloud import _should_test_aws, TEST_AWS_REPO_BUCKET
from tests.test_data_cloud import _should_test_gcp, TEST_GCP_REPO_BUCKET


class TestRepro(TestDvc):
    def setUp(self):
        super(TestRepro, self).setUp()

        self.foo_stage = self.dvc.add(self.FOO)

        self.file1 = 'file1'
        self.file1_stage = self.file1 + '.dvc'
        self.dvc.run(fname=self.file1_stage,
                     outs=[self.file1],
                     deps=[self.FOO, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE, self.FOO, self.file1))


class TestReproNoDeps(TestRepro):
    def test(self):
        out = 'out'
        code_file = 'out.py'
        stage_file = 'out.dvc'
        code = 'import uuid\nwith open("{}", "w+") as fd:\n\tfd.write(str(uuid.uuid4()))\n'.format(out)
        with open(code_file, 'w+') as fd:
            fd.write(code)
        self.dvc.run(fname=stage_file,
                     outs=[out],
                     cmd='python {}'.format(code_file))

        stages = self.dvc.reproduce(stage_file)
        self.assertEqual(len(stages), 1)


class TestReproForce(TestRepro):
    def test(self):
        stages = self.dvc.reproduce(self.file1_stage, force=True)
        self.assertEqual(len(stages), 2)


class TestReproChangedCode(TestRepro):
    def test(self):
        self.swap_code()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertEqual(len(stages), 1)

    def swap_code(self):
        os.unlink(self.CODE)
        new_contents = self.CODE_CONTENTS
        new_contents += "\nshutil.copyfile('{}', sys.argv[2])\n".format(self.BAR)
        self.create(self.CODE, new_contents)


class TestReproChangedData(TestRepro):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertEqual(len(stages), 2)

    def swap_foo_with_bar(self):
        os.unlink(self.FOO)
        shutil.copyfile(self.BAR, self.FOO)


class TestReproChangedDeepData(TestReproChangedData):
    def test(self):
        file2 = 'file2'
        file2_stage = file2 + '.dvc'
        self.dvc.run(fname=file2_stage,
                     outs=[file2],
                     deps=[self.file1, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE, self.file1, file2))

        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(file2_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))
        self.assertTrue(filecmp.cmp(file2, self.BAR))
        self.assertEqual(len(stages), 3)


class TestReproPhony(TestReproChangedData):
    def test(self):
        stage = self.dvc.run(deps=[self.file1])

        self.swap_foo_with_bar()

        self.dvc.reproduce(stage.path)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR))


class TestNonExistingOutput(TestRepro):
    def test(self):
        os.unlink(self.FOO)

        with self.assertRaises(ReproductionError) as cx:
            self.dvc.reproduce(self.file1_stage)


class TestReproDataSource(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.foo_stage.path)

        self.assertTrue(filecmp.cmp(self.FOO, self.BAR))
        self.assertEqual(stages[0].outs[0].md5, file_md5(self.BAR)[0])


class TestReproChangedDir(TestDvc):
    def test(self):
        file_name = 'file'
        shutil.copyfile(self.FOO, file_name)

        stage_name = 'dir.dvc'
        dir_name = 'dir'
        dir_code = 'dir.py'

        with open(dir_code, 'w+') as fd:
            fd.write("import os; import shutil; os.mkdir(\"{}\"); shutil.copyfile(\"{}\", os.path.join(\"{}\", \"{}\"))".format(dir_name, file_name, dir_name, file_name))

        self.dvc.run(fname=stage_name,
                     outs=[dir_name],
                     deps=[file_name, dir_code],
                     cmd="python {}".format(dir_code))

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 0)

        os.unlink(file_name)
        shutil.copyfile(self.BAR, file_name)

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 1)


class TestReproMissingMd5InStageFile(TestRepro):
    def test(self):
        with open(self.file1_stage, 'r') as fd:
            d = yaml.load(fd)

        del(d[Stage.PARAM_OUTS][0][RemoteLOCAL.PARAM_MD5])
        del(d[Stage.PARAM_DEPS][0][RemoteLOCAL.PARAM_MD5])

        with open(self.file1_stage, 'w') as fd:
            yaml.dump(d, fd)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 1)


class TestCmdRepro(TestRepro):
    def test(self):
        ret = main(['repro',
                    self.file1_stage])
        self.assertEqual(ret, 0)

        ret = main(['repro',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)


class TestCmdReproChdir(TestDvc):
    def test(self):
        dname = 'dir'
        os.mkdir(dname)
        foo = os.path.join(dname, self.FOO)
        bar = os.path.join(dname, self.BAR)
        code = os.path.join(dname, self.CODE)
        shutil.copyfile(self.FOO, foo)
        shutil.copyfile(self.CODE, code)

        ret = main(['run',
                   '-f', 'Dvcfile',
                   '-c', dname,
                   '-d', self.FOO,
                   '-o', self.BAR,
                   'python {} {} {}'.format(self.CODE, self.FOO, self.BAR)])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar))

        os.unlink(bar)

        ret = main(['repro',
                    '-c', dname])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar))


class TestReproExternalBase(TestDvc):
    def should_test(self):
        return False

    def test(self):
        if not self.should_test():
            return

        cache = self.scheme + '://' + self.bucket + '/' + str(uuid.uuid4())

        ret = main(['config', 'cache.' + self.scheme, 'myrepo'])
        self.assertEqual(ret, 0)
        ret = main(['remote', 'add', 'myrepo', cache])
        self.assertEqual(ret, 0)

        remote_name = 'myremote'
        remote_key = str(uuid.uuid4())
        remote = self.scheme + '://' + self.bucket + '/' + remote_key

        ret = main(['remote', 'add', remote_name, remote])
        self.assertEqual(ret, 0)

        self.dvc = Project('.')

        foo_key = remote_key + '/' + self.FOO
        bar_key = remote_key + '/' + self.BAR

        foo_path = self.scheme + '://' + self.bucket + '/' + foo_key
        bar_path = self.scheme + '://' + self.bucket + '/' + bar_key

        # Using both plain and remote notation
        out_foo_path = 'remote://' + remote_name + '/' + self.FOO
        out_bar_path = bar_path

        self.write(self.bucket, foo_key, 'foo')

        stage = self.dvc.run(outs=[out_bar_path],
                             deps=[out_foo_path],
                             cmd='{} {} {}'.format(self.cmd, foo_path, bar_path))

        self.write(self.bucket, foo_key, 'bar')

        stages = self.dvc.reproduce(stage.path)

        self.assertEqual(len(stages), 1)


class TestReproExternalS3(TestReproExternalBase):
    def should_test(self):
        return _should_test_aws()

    @property
    def scheme(self):
        return 's3'

    @property
    def bucket(self):
        return TEST_AWS_REPO_BUCKET

    @property
    def cmd(self):
        return 'aws s3 cp'

    def write(self, bucket, key, body):
        s3 = boto3.resource('s3')
        s3.Bucket(bucket).put_object(Key=key, Body=body)


class TestReproExternalGS(TestReproExternalBase):
    def should_test(self):
        #FIXME enable
        if os.getenv('CI'):
            return False
        return _should_test_gcp()

    @property
    def scheme(self):
        return 'gs'

    @property
    def bucket(self):
        return TEST_GCP_REPO_BUCKET

    @property
    def cmd(self):
        return 'gsutil cp'

    def write(self, bucket, key, body):
        client = gc.Client()
        bucket = client.bucket(bucket)
        bucket.blob(key).upload_from_string(body)


class TestReproShell(TestDvc):
    def test(self):
        if os.name == 'nt':
            return

        fname = 'shell.txt'
        stage = fname + '.dvc'

        self.dvc.run(fname=stage,
                     outs=[fname],
                     cmd='echo $SHELL > {}'.format(fname))

        with open(fname, 'r') as fd:
            self.assertEqual(os.getenv('SHELL'), fd.read().strip())

        os.unlink(fname)

        self.dvc.reproduce(stage)

        with open(fname, 'r') as fd:
            self.assertEqual(os.getenv('SHELL'), fd.read().strip())
