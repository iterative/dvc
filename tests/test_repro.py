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
from dvc.project import ReproductionError
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


class TestReproExternalBase(TestDvc):
    def should_test(self):
        return False

    def test(self):
        if not self.should_test():
            return

        foo_key = str(uuid.uuid4()) + '/' + self.FOO
        bar_key = str(uuid.uuid4()) + '/' + self.BAR

        foo_path = self.schema + self.bucket + '/' + foo_key
        bar_path = self.schema + self.bucket + '/' + bar_key

        self.write(self.bucket, foo_key, 'foo')

        stage = self.dvc.run(outs_no_cache=[bar_path],
                             deps=[foo_path],
                             cmd='{} {} {}'.format(self.cmd, foo_path, bar_path))

        self.write(self.bucket, foo_key, 'bar')

        stages = self.dvc.reproduce(stage.path)

        self.assertEqual(len(stages), 1)


class TestReproExternalS3(TestReproExternalBase):
    def should_test(self):
        return _should_test_aws()

    @property
    def schema(self):
        return 's3://'

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
    def schema(self):
        return 'gs://'

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
