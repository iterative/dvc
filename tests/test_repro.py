import os
import yaml
import shutil
import filecmp
import getpass
import posixpath
from subprocess import Popen, PIPE

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

import boto3
import uuid
from google.cloud import storage as gc

from dvc.main import main
from dvc.project import Project, ReproductionError
from dvc.utils import file_md5
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage, StageFileDoesNotExistError
from dvc.system import System

from tests.basic_env import TestDvc
from tests.test_data_cloud import _should_test_aws, TEST_AWS_REPO_BUCKET
from tests.test_data_cloud import _should_test_gcp, TEST_GCP_REPO_BUCKET
from tests.test_data_cloud import _should_test_ssh, _should_test_hdfs
from tests.test_data_cloud import sleep
from tests.utils.httpd import StaticFileServer


class TestRepro(TestDvc):
    def setUp(self):
        super(TestRepro, self).setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        self.file1 = 'file1'
        self.file1_stage = self.file1 + '.dvc'
        self.dvc.run(fname=self.file1_stage,
                     outs=[self.file1],
                     deps=[self.FOO, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.FOO,
                                                  self.file1))


class TestReproFail(TestRepro):
    def test(self):
        os.unlink(self.CODE)

        ret = main(['repro', self.file1_stage])
        self.assertNotEqual(ret, 0)


class TestReproDepUnderDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.dir_stage = stages[0]
        self.assertTrue(self.dir_stage is not None)

        sleep()

        self.file1 = 'file1'
        self.file1_stage = self.file1 + '.dvc'
        self.dvc.run(fname=self.file1_stage,
                     outs=[self.file1],
                     deps=[self.DATA, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.DATA,
                                                  self.file1))

        self.assertTrue(filecmp.cmp(self.file1, self.DATA, shallow=False))

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        sleep()

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 2)
        self.assertTrue(filecmp.cmp(self.file1, self.FOO, shallow=False))


class TestReproDepDirWithOutputsUnderIt(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        sleep()

        stages = self.dvc.add(self.DATA_SUB)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        sleep()

        stage = self.dvc.run(fname='Dvcfile',
                             deps=[self.DATA, self.DATA_SUB])
        self.assertTrue(stage is not None)

        sleep()

        file1 = 'file1'
        file1_stage = file1 + '.dvc'
        stage = self.dvc.run(fname=file1_stage,
                             deps=[self.DATA_DIR],
                             outs=[file1],
                             cmd='python {} {} {}'.format(self.CODE,
                                                          self.DATA,
                                                          file1))
        self.assertTrue(stage is not None)

        sleep()

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        sleep()

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 2)


class TestReproNoDeps(TestRepro):
    def test(self):
        out = 'out'
        code_file = 'out.py'
        stage_file = 'out.dvc'
        code = 'import uuid\nwith open("{}", "w+") as fd:\n' \
               '\tfd.write(str(uuid.uuid4()))\n'.format(out)
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

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 1)

    def swap_code(self):
        os.unlink(self.CODE)
        new_contents = self.CODE_CONTENTS
        new_contents += "\nshutil.copyfile('{}', " \
                        "sys.argv[2])\n".format(self.BAR)
        self.create(self.CODE, new_contents)


class TestReproChangedData(TestRepro):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertEqual(len(stages), 2)

    def swap_foo_with_bar(self):
        os.unlink(self.FOO)
        shutil.copyfile(self.BAR, self.FOO)


class TestReproDry(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file1_stage, dry=True)

        self.assertTrue(len(stages), 2)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))

        ret = main(['repro', '--dry', self.file1_stage])
        self.assertEqual(ret, 0)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestReproUpToDate(TestRepro):
    def test(self):
        ret = main(['repro', self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproDryNoExec(TestDvc):
    def test(self):
        deps = []
        for d in range(3):
            idir = 'idir{}'.format(d)
            odir = 'odir{}'.format(d)

            deps.append('-d')
            deps.append(odir)

            os.mkdir(idir)

            f = os.path.join(idir, 'file')
            with open(f, 'w+') as fobj:
                fobj.write(str(d))

            ret = main(['run',
                        '--no-exec',
                        '-d', idir,
                        '-o', odir,
                        "python -c 'import shutil; "
                        "shutil.copytree(\"{}\", \"{}\")'".format(idir, odir)])
            self.assertEqual(ret, 0)

        ret = main(['run', '--no-exec', '-f', 'Dvcfile'] + deps)
        self.assertEqual(ret, 0)

        ret = main(['repro', '--dry'])
        self.assertEqual(ret, 0)


class TestReproChangedDeepData(TestReproChangedData):
    def setUp(self):
        super(TestReproChangedDeepData, self).setUp()

        self.file2 = 'file2'
        self.file2_stage = self.file2 + '.dvc'
        self.dvc.run(fname=self.file2_stage,
                     outs=[self.file2],
                     deps=[self.file1, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.file1,
                                                  self.file2))

    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.file2_stage)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(self.file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)


class TestReproIgnoreBuildCache(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        foo_stage = stages[0]
        self.assertTrue(foo_stage is not None)

        code1 = 'code1.py'
        shutil.copyfile(self.CODE, code1)
        file1 = 'file1'
        file1_stage = file1 + '.dvc'
        stage = self.dvc.run(fname=file1_stage,
                             outs=[file1],
                             deps=[self.FOO, code1],
                             cmd='python {} {} {}'.format(code1,
                                                          self.FOO,
                                                          file1))
        self.assertTrue(stage is not None)

        code2 = 'code2.py'
        shutil.copyfile(self.CODE, code2)
        file2 = 'file2'
        file2_stage = file2 + '.dvc'
        stage = self.dvc.run(fname=file2_stage,
                             outs=[file2],
                             deps=[file1, code2],
                             cmd='python {} {} {}'.format(code2,
                                                          file1,
                                                          file2))
        self.assertTrue(stage is not None)

        sleep()
        
        with open(code1, 'a') as fobj:
            fobj.write('\n\n')

        stages = self.dvc.reproduce(file2_stage)
        self.assertEqual(len(stages), 1)

        stages = self.dvc.reproduce(file2_stage, ignore_build_cache=True)
        self.assertEqual(len(stages), 2)


class TestReproPipeline(TestReproChangedDeepData):
    def test(self):
        stages = self.dvc.reproduce(self.file1_stage,
                                    force=True,
                                    pipeline=True)
        self.assertEqual(len(stages), 3)

    def test_cli(self):
        ret = main(['repro', '--pipeline', '-f', self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproPipelines(TestDvc):
    def setUp(self):
        super(TestReproPipelines, self).setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        stages = self.dvc.add(self.BAR)
        self.assertEqual(len(stages), 1)
        self.bar_stage = stages[0]
        self.assertTrue(self.bar_stage is not None)

        self.file1 = 'file1'
        self.file1_stage = self.file1 + '.dvc'
        self.dvc.run(fname=self.file1_stage,
                     outs=[self.file1],
                     deps=[self.FOO, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.FOO,
                                                  self.file1))

        self.file2 = 'file2'
        self.file2_stage = self.file2 + '.dvc'
        self.dvc.run(fname=self.file2_stage,
                     outs=[self.file2],
                     deps=[self.BAR, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.BAR,
                                                  self.file2))

    def test(self):
        stages = self.dvc.reproduce(all_pipelines=True, force=True)
        self.assertEqual(len(stages), 4)
        names = [stage.relpath for stage in stages]
        self.assertTrue(self.foo_stage.relpath in names)
        self.assertTrue(self.bar_stage.relpath in names)
        self.assertTrue(self.file1_stage in names)
        self.assertTrue(self.file2_stage in names)

    def test_cli(self):
        ret = main(['repro', '-f', '-P'])
        self.assertEqual(ret, 0)


class TestReproLocked(TestReproChangedData):
    def test(self):
        file2 = 'file2'
        file2_stage = file2 + '.dvc'
        self.dvc.run(fname=file2_stage,
                     outs=[file2],
                     deps=[self.file1, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE,
                                                  self.file1,
                                                  file2))

        self.swap_foo_with_bar()

        ret = main(['lock', file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertEqual(len(stages), 0)

        ret = main(['unlock', file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)

    def test_non_existing(self):
        with self.assertRaises(StageFileDoesNotExistError):
            self.dvc.lock_stage('non-existing-stage')

        ret = main(['lock', 'non-existing-stage'])
        self.assertNotEqual(ret, 0)


class TestReproLockedCallback(TestDvc):
    def test(self):
        file1 = 'file1'
        file1_stage = file1 + '.dvc'
        # NOTE: purposefully not specifying dependencies
        # to create a callbacs stage.
        stage = self.dvc.run(fname=file1_stage,
                             outs=[file1],
                             cmd='python {} {} {}'.format(self.CODE,
                                                          self.FOO,
                                                          file1))
        self.assertTrue(stage is not None)
        self.assertEqual(stage.relpath, file1_stage)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 1)

        self.dvc.lock_stage(file1_stage)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.lock_stage(file1_stage, unlock=True)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 1)


class TestReproLockedUnchanged(TestRepro):
    def test(self):
        """
        Check that locking/unlocking doesn't affect stage state
        """
        self.dvc.lock_stage(self.file1_stage)
        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.lock_stage(self.file1_stage, unlock=True)
        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 0)


class TestReproMetricsAddUnchanged(TestDvc):
    def test(self):
        """
        Check that adding/removing metrics doesn't affect stage state
        """
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        file1 = 'file1'
        file1_stage = file1 + '.dvc'
        self.dvc.run(fname=file1_stage,
                     outs_no_cache=[file1],
                     deps=[self.FOO, self.CODE],
                     cmd='python {} {} {}'.format(self.CODE, self.FOO, file1))

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics_add(file1)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics_remove(file1)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)


class TestReproPhony(TestReproChangedData):
    def test(self):
        stage = self.dvc.run(deps=[self.file1])

        self.swap_foo_with_bar()

        self.dvc.reproduce(stage.path)

        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestNonExistingOutput(TestRepro):
    def test(self):
        os.unlink(self.FOO)

        with self.assertRaises(ReproductionError):
            self.dvc.reproduce(self.file1_stage)


class TestReproDataSource(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        stages = self.dvc.reproduce(self.foo_stage.path)

        self.assertTrue(filecmp.cmp(self.FOO, self.BAR, shallow=False))
        self.assertEqual(stages[0].outs[0].md5, file_md5(self.BAR)[0])


class TestReproChangedDir(TestDvc):
    def test(self):
        file_name = 'file'
        shutil.copyfile(self.FOO, file_name)

        stage_name = 'dir.dvc'
        dir_name = 'dir'
        dir_code = 'dir.py'
        code = "import os; import shutil; os.mkdir(\"{}\"); " \
               "shutil.copyfile(\"{}\", os.path.join(\"{}\", \"{}\"))"

        with open(dir_code, 'w+') as fd:
            fd.write(code.format(dir_name, file_name, dir_name, file_name))

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


class TestReproChangedDirData(TestDvc):
    def test(self):
        dir_name = 'dir'
        dir_code = 'dir_code.py'

        sleep()

        with open(dir_code, 'w+') as fd:
            fd.write("import os; import sys; import shutil; "
                     "shutil.copytree(sys.argv[1], sys.argv[2])")

        sleep()

        stage = self.dvc.run(outs=[dir_name],
                             deps=[self.DATA_DIR, dir_code],
                             cmd="python {} {} {}".format(dir_code,
                                                          self.DATA_DIR,
                                                          dir_name))
        self.assertTrue(stage is not None)

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 0)

        sleep()

        with open(self.DATA_SUB, 'a') as fd:
            fd.write('add')

        sleep()

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        sleep()

        # Check that dvc indeed registers changed output dir
        shutil.move(self.BAR, dir_name)
        sleep()
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc registers mtime change for the directory.
        System.hardlink(self.DATA_SUB, self.DATA_SUB + '.lnk')
        sleep()
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


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


class TestCmdRepro(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        ret = main(['status'])
        self.assertEqual(ret, 0)

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
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))

        os.unlink(bar)

        ret = main(['repro',
                    '-c', dname])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))


class TestReproExternalBase(TestDvc):
    def should_test(self):
        return False

    @property
    def cache_scheme(self):
        return self.scheme

    @property
    def scheme(self):
        return None

    @property
    def scheme_sep(self):
        return '://'

    @property
    def sep(self):
        return '/'

    def test(self):
        if not self.should_test():
            return

        cache = (self.scheme + self.scheme_sep + self.bucket + self.sep
                 + str(uuid.uuid4()))

        ret = main(['config', 'cache.' + self.cache_scheme, 'myrepo'])
        self.assertEqual(ret, 0)
        ret = main(['remote', 'add', 'myrepo', cache])
        self.assertEqual(ret, 0)

        remote_name = 'myremote'
        remote_key = str(uuid.uuid4())
        remote = (self.scheme + self.scheme_sep + self.bucket + self.sep
                  + remote_key)

        ret = main(['remote', 'add', remote_name, remote])
        self.assertEqual(ret, 0)

        self.dvc = Project('.')
        self.dvc.logger.be_verbose()

        foo_key = remote_key + self.sep + self.FOO
        bar_key = remote_key + self.sep + self.BAR

        foo_path = (self.scheme + self.scheme_sep + self.bucket + self.sep
                    + foo_key)
        bar_path = (self.scheme + self.scheme_sep + self.bucket + self.sep
                    + bar_key)

        # Using both plain and remote notation
        out_foo_path = 'remote://' + remote_name + '/' + self.FOO
        out_bar_path = bar_path

        self.write(self.bucket, foo_key, self.FOO_CONTENTS)

        sleep()

        import_stage = self.dvc.imp(out_foo_path, 'import')
        self.assertTrue(os.path.exists('import'))
        self.assertTrue(filecmp.cmp('import', self.FOO, shallow=False))
        self.assertEqual(self.dvc.status(import_stage.path), {})

        import_remote_stage = self.dvc.imp(out_foo_path,
                                           out_foo_path + '_imported')
        self.assertEqual(self.dvc.status(import_remote_stage.path), {})

        cmd_stage = self.dvc.run(outs=[out_bar_path],
                                 deps=[out_foo_path],
                                 cmd=self.cmd(foo_path, bar_path))
        self.assertEqual(self.dvc.status(cmd_stage.path), {})

        self.assertEqual(self.dvc.status(), {})

        self.write(self.bucket, foo_key, self.BAR_CONTENTS)

        sleep()

        self.assertNotEqual(self.dvc.status(), {})

        stages = self.dvc.reproduce(import_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(os.path.exists('import'))
        self.assertTrue(filecmp.cmp('import', self.BAR, shallow=False))
        self.assertEqual(self.dvc.status(import_stage.path), {})

        stages = self.dvc.reproduce(import_remote_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertEqual(self.dvc.status(import_remote_stage.path), {})

        stages = self.dvc.reproduce(cmd_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertEqual(self.dvc.status(cmd_stage.path), {})

        self.assertEqual(self.dvc.status(), {})
        self.dvc.gc()
        self.assertEqual(self.dvc.status(), {})

        self.dvc.remove(cmd_stage.path, outs_only=True)
        self.assertNotEqual(self.dvc.status(cmd_stage.path), {})

        self.dvc.checkout(cmd_stage.path, force=True)
        self.assertEqual(self.dvc.status(cmd_stage.path), {})


class TestReproExternalS3(TestReproExternalBase):
    def should_test(self):
        return _should_test_aws()

    @property
    def scheme(self):
        return 's3'

    @property
    def bucket(self):
        return TEST_AWS_REPO_BUCKET

    def cmd(self, i, o):
        return 'aws s3 cp {} {}'.format(i, o)

    def write(self, bucket, key, body):
        s3 = boto3.resource('s3')
        s3.Bucket(bucket).put_object(Key=key, Body=body)


class TestReproExternalGS(TestReproExternalBase):
    def should_test(self):
        return _should_test_gcp()

    @property
    def scheme(self):
        return 'gs'

    @property
    def bucket(self):
        return TEST_GCP_REPO_BUCKET

    def cmd(self, i, o):
        return 'gsutil cp {} {}'.format(i, o)

    def write(self, bucket, key, body):
        client = gc.Client()
        bucket = client.bucket(bucket)
        bucket.blob(key).upload_from_string(body)


class TestReproExternalHDFS(TestReproExternalBase):
    def should_test(self):
        return _should_test_hdfs()

    @property
    def scheme(self):
        return 'hdfs'

    @property
    def bucket(self):
        return '{}@127.0.0.1'.format(getpass.getuser())

    def cmd(self, i, o):
        return 'hadoop fs -cp {} {}'.format(i, o)

    def write(self, bucket, key, body):
        url = self.scheme + '://' + bucket + '/' + key
        p = Popen('hadoop fs -rm -f {}'.format(url),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        p.communicate()

        p = Popen('hadoop fs -mkdir -p {}'.format(posixpath.dirname(url)),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)

        with open('tmp', 'w+') as fd:
            fd.write(body)

        p = Popen('hadoop fs -copyFromLocal {} {}'.format('tmp', url),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)


class TestReproExternalSSH(TestReproExternalBase):
    _dir = None

    def should_test(self):
        return _should_test_ssh()

    @property
    def scheme(self):
        return 'ssh'

    @property
    def bucket(self):
        if not self._dir:
            self._dir = TestDvc.mkdtemp()
        return '{}@127.0.0.1:{}'.format(getpass.getuser(), self._dir)

    def cmd(self, i, o):
        i = i.strip('ssh://')
        o = o.strip('ssh://')
        return 'scp {} {}'.format(i, o)

    def write(self, bucket, key, body):
        dest = '{}@127.0.0.1'.format(getpass.getuser())
        path = posixpath.join(self._dir, key)
        p = Popen('ssh {} rm {}'.format(dest, path),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        p.communicate()

        p = Popen('ssh {} "mkdir -p $(dirname {})"'.format(dest, path),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)

        p = Popen('echo "{}" | ssh {} "tr -d \'\n\' > {}"'.format(body,
                                                                  dest,
                                                                  path),
                  shell=True,
                  executable=os.getenv('SHELL'),
                  stdin=PIPE,
                  stdout=PIPE,
                  stderr=PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)


class TestReproExternalLOCAL(TestReproExternalBase):
    def setUp(self):
        super(TestReproExternalLOCAL, self).setUp()
        self.tmpdir = TestDvc.mkdtemp()

    def should_test(self):
        return True

    @property
    def cache_scheme(self):
        return 'local'

    @property
    def scheme(self):
        return ''

    @property
    def scheme_sep(self):
        return ''

    @property
    def sep(self):
        return os.sep

    @property
    def bucket(self):
        return self.tmpdir

    def cmd(self, i, o):
        if os.name == 'nt':
            return 'copy {} {}'.format(i, o)
        return 'cp {} {}'.format(i, o)

    def write(self, bucket, key, body):
        path = os.path.join(bucket, key)
        dname = os.path.dirname(path)

        if not os.path.exists(dname):
            os.makedirs(dname)

        with open(path, 'w+') as fd:
            fd.write(body)


class TestReproExternalHTTP(TestReproExternalBase):
    _external_cache_id = None

    @property
    def remote(self):
        return 'http://localhost:8000/'

    @property
    def local_cache(self):
        return os.path.join(self.dvc.dvc_dir, 'cache')

    @property
    def external_cache_id(self):
        if not self._external_cache_id:
            self._external_cache_id = str(uuid.uuid4())

        return self._external_cache_id

    @property
    def external_cache(self):
        return urljoin(self.remote, self.external_cache_id)

    def test(self):
        ret1 = main(['remote', 'add', 'mycache', self.external_cache])
        ret2 = main(['remote', 'add', 'myremote', self.remote])
        self.assertEqual(ret1, 0)
        self.assertEqual(ret2, 0)

        self.dvc = Project('.')

        # Import
        with StaticFileServer():
            import_url = urljoin(self.remote, self.FOO)
            import_output = 'imported_file'
            import_stage = self.dvc.imp(import_url, import_output)

        self.assertTrue(os.path.exists(import_output))
        self.assertTrue(filecmp.cmp(import_output, self.FOO, shallow=False))

        # Run --deps
        with StaticFileServer():
            run_dependency = urljoin(self.remote, self.BAR)
            run_output = 'remote_file'
            cmd = 'open("{}", "w+")'.format(run_output)

            with open('create-output.py', 'w') as fd:
                fd.write(cmd)

            run_stage = self.dvc.run(deps=[run_dependency],
                                     outs=[run_output],
                                     cmd='python create-output.py')
            self.assertTrue(run_stage is not None)

        self.assertTrue(os.path.exists(run_output))

        # Pull
        self.dvc.remove(import_stage.path, outs_only=True)
        self.assertFalse(os.path.exists(import_output))

        shutil.move(self.local_cache, self.external_cache_id)
        self.assertFalse(os.path.exists(self.local_cache))

        with StaticFileServer():
            self.dvc.pull(import_stage.path, remote='mycache')

        self.assertTrue(os.path.exists(import_output))


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


class TestReproNoSCM(TestRepro):
    def test(self):
        shutil.rmtree(self.dvc.scm.dir)
        ret = main(['repro', self.file1_stage])
        self.assertEqual(ret, 0)
