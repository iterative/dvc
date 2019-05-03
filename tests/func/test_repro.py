from __future__ import unicode_literals
from dvc.utils.compat import str, urljoin, Path

import os
import re
import shutil
import filecmp
import getpass
import posixpath
from subprocess import Popen, PIPE

import boto3
import uuid
import paramiko
from google.cloud import storage as gc
from flaky.flaky_decorator import flaky
import pytest

from dvc.main import main
from dvc.repo import Repo as DvcRepo
from dvc.utils import file_md5
from dvc.utils.stage import load_stage_file, dump_stage_file
from dvc.remote.local import RemoteLOCAL
from dvc.stage import Stage, StageFileDoesNotExistError
from dvc.system import System
from dvc.output.base import OutputBase
from dvc.exceptions import (
    CyclicGraphError,
    StagePathAsOutputError,
    ReproductionError,
)

from tests.basic_env import TestDvc
from tests.func.test_data_cloud import _should_test_aws, TEST_AWS_REPO_BUCKET
from tests.func.test_data_cloud import _should_test_gcp, TEST_GCP_REPO_BUCKET
from tests.func.test_data_cloud import _should_test_ssh, _should_test_hdfs
from tests.utils.httpd import StaticFileServer
from mock import patch


class TestRepro(TestDvc):
    def setUp(self):
        super(TestRepro, self).setUp()

        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.foo_stage = stages[0]
        self.assertTrue(self.foo_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, self.file1),
        )


class TestReproFail(TestRepro):
    def test(self):
        os.unlink(self.CODE)

        ret = main(["repro", self.file1_stage])
        self.assertNotEqual(ret, 0)


class TestReproCyclicGraph(TestDvc):
    def test(self):
        self.dvc.run(
            deps=[self.FOO], outs=["bar.txt"], cmd="echo bar > bar.txt"
        )

        self.dvc.run(
            deps=["bar.txt"], outs=["baz.txt"], cmd="echo baz > baz.txt"
        )

        stage_dump = {
            "cmd": "echo baz > foo",
            "deps": [{"path": "baz.txt"}],
            "outs": [{"path": self.FOO}],
        }
        dump_stage_file("cycle.dvc", stage_dump)

        with self.assertRaises(CyclicGraphError):
            self.dvc.reproduce("cycle.dvc")


class TestReproWorkingDirectoryAsOutput(TestDvc):
    """
    |  stage.cwd  |  out.path | cwd as output |
    |:-----------:|:---------:|:-------------:|
    |     dir     |    dir    |      True     |
    | dir/subdir/ |    dir    |      True     |
    |     dir     |   dir-1   |     False     |
    |      .      | something |     False     |
    """

    def test(self):
        # File structure:
        #       .
        #       |-- dir1
        #       |  |__ dir2.dvc         (out.path == ../dir2)
        #       |__ dir2
        #           |__ something.dvc    (stage.cwd == ./dir2)

        os.mkdir(os.path.join(self.dvc.root_dir, "dir1"))

        self.dvc.run(
            cwd="dir1",
            outs=["../dir2"],
            cmd="mkdir {path}".format(path=os.path.join("..", "dir2")),
        )

        faulty_stage_path = os.path.join("dir2", "something.dvc")

        output = os.path.join("..", "something")
        stage_dump = {
            "cmd": "echo something > {}".format(output),
            "outs": [{"path": output}],
        }
        dump_stage_file(faulty_stage_path, stage_dump)

        with self.assertRaises(StagePathAsOutputError):
            self.dvc.reproduce(faulty_stage_path)

    def test_nested(self):
        from dvc.stage import Stage

        #
        #       .
        #       |-- a
        #       |  |__ nested
        #       |     |__ dir
        #       |       |__ error.dvc     (stage.cwd == 'a/nested/dir')
        #       |__ b
        #          |__ nested.dvc         (stage.out == 'a/nested')
        dir1 = "b"
        dir2 = "a"

        os.mkdir(dir1)
        os.mkdir(dir2)

        nested_dir = os.path.join(dir2, "nested")
        out_dir = os.path.relpath(nested_dir, dir1)

        nested_stage = self.dvc.run(
            cwd=dir1,  # b
            outs=[out_dir],  # ../a/nested
            cmd="mkdir {path}".format(path=out_dir),
        )

        os.mkdir(os.path.join(nested_dir, "dir"))

        error_stage_path = os.path.join(nested_dir, "dir", "error.dvc")

        output = os.path.join("..", "..", "something")
        stage_dump = {
            "cmd": "echo something > {}".format(output),
            "outs": [{"path": output}],
        }
        dump_stage_file(error_stage_path, stage_dump)

        # NOTE: os.walk() walks in a sorted order and we need dir2 subdirs to
        # be processed before dir1 to load error.dvc first.
        with patch.object(DvcRepo, "stages") as mock_stages:
            mock_stages.return_value = [
                nested_stage,
                Stage.load(self.dvc, error_stage_path),
            ]

            with self.assertRaises(StagePathAsOutputError):
                self.dvc.reproduce(error_stage_path)

    def test_similar_paths(self):
        # File structure:
        #
        #       .
        #       |-- something.dvc   (out.path == something)
        #       |-- something
        #       |__ something-1
        #          |-- a
        #          |__ a.dvc        (stage.cwd == something-1)

        self.dvc.run(outs=["something"], cmd="mkdir something")

        os.mkdir("something-1")

        stage = os.path.join("something-1", "a.dvc")

        stage_dump = {"cmd": "echo a > a", "outs": [{"path": "a"}]}
        dump_stage_file(stage, stage_dump)

        try:
            self.dvc.reproduce(stage)
        except StagePathAsOutputError:
            self.fail("should not raise StagePathAsOutputError")


class TestReproDepUnderDir(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.dir_stage = stages[0]
        self.assertTrue(self.dir_stage is not None)

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.DATA, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.DATA, self.file1),
        )

        self.assertTrue(filecmp.cmp(self.file1, self.DATA, shallow=False))

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 2)
        self.assertTrue(filecmp.cmp(self.file1, self.FOO, shallow=False))


class TestReproDepDirWithOutputsUnderIt(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        stages = self.dvc.add(self.DATA_SUB)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        stage = self.dvc.run(fname="Dvcfile", deps=[self.DATA, self.DATA_SUB])
        self.assertTrue(stage is not None)

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        stage = self.dvc.run(
            fname=file1_stage,
            deps=[self.DATA_DIR],
            outs=[file1],
            cmd="python {} {} {}".format(self.CODE, self.DATA, file1),
        )
        self.assertTrue(stage is not None)

        os.unlink(self.DATA)
        shutil.copyfile(self.FOO, self.DATA)

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 2)


class TestReproNoDeps(TestRepro):
    def test(self):
        out = "out"
        code_file = "out.py"
        stage_file = "out.dvc"
        code = (
            'import uuid\nwith open("{}", "w+") as fd:\n'
            "\tfd.write(str(uuid.uuid4()))\n".format(out)
        )
        with open(code_file, "w+") as fd:
            fd.write(code)
        self.dvc.run(
            fname=stage_file, outs=[out], cmd="python {}".format(code_file)
        )

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
        new_contents += "\nshutil.copyfile('{}', " "sys.argv[2])\n".format(
            self.BAR
        )
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

        ret = main(["repro", "--dry", self.file1_stage])
        self.assertEqual(ret, 0)
        self.assertFalse(filecmp.cmp(self.file1, self.BAR, shallow=False))


class TestReproUpToDate(TestRepro):
    def test(self):
        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproDryNoExec(TestDvc):
    def test(self):
        deps = []
        for d in range(3):
            idir = "idir{}".format(d)
            odir = "odir{}".format(d)

            deps.append("-d")
            deps.append(odir)

            os.mkdir(idir)

            f = os.path.join(idir, "file")
            with open(f, "w+") as fobj:
                fobj.write(str(d))

            ret = main(
                [
                    "run",
                    "--no-exec",
                    "-d",
                    idir,
                    "-o",
                    odir,
                    "python -c 'import shutil; "
                    'shutil.copytree("{}", "{}")\''.format(idir, odir),
                ]
            )
            self.assertEqual(ret, 0)

        ret = main(["run", "--no-exec", "-f", "Dvcfile"] + deps)
        self.assertEqual(ret, 0)

        ret = main(["repro", "--dry"])
        self.assertEqual(ret, 0)


class TestReproChangedDeepData(TestReproChangedData):
    def setUp(self):
        super(TestReproChangedDeepData, self).setUp()

        self.file2 = "file2"
        self.file2_stage = self.file2 + ".dvc"
        self.dvc.run(
            fname=self.file2_stage,
            outs=[self.file2],
            deps=[self.file1, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.file1, self.file2),
        )

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

        code1 = "code1.py"
        shutil.copyfile(self.CODE, code1)
        file1 = "file1"
        file1_stage = self.dvc.run(
            outs=[file1],
            deps=[self.FOO, code1],
            cmd="python {} {} {}".format(code1, self.FOO, file1),
        )
        self.assertTrue(file1_stage is not None)

        code2 = "code2.py"
        shutil.copyfile(self.CODE, code2)
        file2 = "file2"
        file2_stage = self.dvc.run(
            outs=[file2],
            deps=[file1, code2],
            cmd="python {} {} {}".format(code2, file1, file2),
        )
        self.assertTrue(file2_stage is not None)

        code3 = "code3.py"
        shutil.copyfile(self.CODE, code3)
        file3 = "file3"
        file3_stage = self.dvc.run(
            outs=[file3],
            deps=[file2, code3],
            cmd="python {} {} {}".format(code3, file2, file3),
        )
        self.assertTrue(file3_stage is not None)

        with open(code2, "a") as fobj:
            fobj.write("\n\n")

        stages = self.dvc.reproduce(file3_stage.path, ignore_build_cache=True)
        self.assertEqual(len(stages), 2)
        self.assertEqual(stages[0].path, file2_stage.path)
        self.assertEqual(stages[1].path, file3_stage.path)


class TestReproPipeline(TestReproChangedDeepData):
    def test(self):
        stages = self.dvc.reproduce(
            self.file1_stage, force=True, pipeline=True
        )
        self.assertEqual(len(stages), 3)

    def test_cli(self):
        ret = main(["repro", "--pipeline", "-f", self.file1_stage])
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

        self.file1 = "file1"
        self.file1_stage = self.file1 + ".dvc"
        self.dvc.run(
            fname=self.file1_stage,
            outs=[self.file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, self.file1),
        )

        self.file2 = "file2"
        self.file2_stage = self.file2 + ".dvc"
        self.dvc.run(
            fname=self.file2_stage,
            outs=[self.file2],
            deps=[self.BAR, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.BAR, self.file2),
        )

    def test(self):
        stages = self.dvc.reproduce(all_pipelines=True, force=True)
        self.assertEqual(len(stages), 4)
        names = [stage.relpath for stage in stages]
        self.assertTrue(self.foo_stage.relpath in names)
        self.assertTrue(self.bar_stage.relpath in names)
        self.assertTrue(self.file1_stage in names)
        self.assertTrue(self.file2_stage in names)

    def test_cli(self):
        ret = main(["repro", "-f", "-P"])
        self.assertEqual(ret, 0)


class TestReproLocked(TestReproChangedData):
    def test(self):
        file2 = "file2"
        file2_stage = file2 + ".dvc"
        self.dvc.run(
            fname=file2_stage,
            outs=[file2],
            deps=[self.file1, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.file1, file2),
        )

        self.swap_foo_with_bar()

        ret = main(["lock", file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertEqual(len(stages), 0)

        ret = main(["unlock", file2_stage])
        self.assertEqual(ret, 0)
        stages = self.dvc.reproduce(file2_stage)
        self.assertTrue(filecmp.cmp(self.file1, self.BAR, shallow=False))
        self.assertTrue(filecmp.cmp(file2, self.BAR, shallow=False))
        self.assertEqual(len(stages), 3)

    def test_non_existing(self):
        with self.assertRaises(StageFileDoesNotExistError):
            self.dvc.lock_stage("non-existing-stage")

        ret = main(["lock", "non-existing-stage"])
        self.assertNotEqual(ret, 0)


class TestReproLockedCallback(TestDvc):
    def test(self):
        file1 = "file1"
        file1_stage = file1 + ".dvc"
        # NOTE: purposefully not specifying dependencies
        # to create a callbacs stage.
        stage = self.dvc.run(
            fname=file1_stage,
            outs=[file1],
            cmd="python {} {} {}".format(self.CODE, self.FOO, file1),
        )
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

        file1 = "file1"
        file1_stage = file1 + ".dvc"
        self.dvc.run(
            fname=file1_stage,
            outs_no_cache=[file1],
            deps=[self.FOO, self.CODE],
            cmd="python {} {} {}".format(self.CODE, self.FOO, file1),
        )

        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics.add(file1)
        stages = self.dvc.reproduce(file1_stage)
        self.assertEqual(len(stages), 0)

        self.dvc.metrics.remove(file1)
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
        self.assertEqual(stages[0].outs[0].checksum, file_md5(self.BAR)[0])


class TestReproChangedDir(TestDvc):
    def test(self):
        file_name = "file"
        shutil.copyfile(self.FOO, file_name)

        stage_name = "dir.dvc"
        dir_name = "dir"
        dir_code = "dir.py"
        code = (
            'import os; import shutil; os.mkdir("{}"); '
            'shutil.copyfile("{}", os.path.join("{}", "{}"))'
        )

        with open(dir_code, "w+") as fd:
            fd.write(code.format(dir_name, file_name, dir_name, file_name))

        self.dvc.run(
            fname=stage_name,
            outs=[dir_name],
            deps=[file_name, dir_code],
            cmd="python {}".format(dir_code),
        )

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 0)

        os.unlink(file_name)
        shutil.copyfile(self.BAR, file_name)

        stages = self.dvc.reproduce(stage_name)
        self.assertEqual(len(stages), 1)


class TestReproChangedDirData(TestDvc):
    def test(self):
        dir_name = "dir"
        dir_code = "dir_code.py"

        with open(dir_code, "w+") as fd:
            fd.write(
                "import os; import sys; import shutil; "
                "shutil.copytree(sys.argv[1], sys.argv[2])"
            )

        stage = self.dvc.run(
            outs=[dir_name],
            deps=[self.DATA_DIR, dir_code],
            cmd="python {} {} {}".format(dir_code, self.DATA_DIR, dir_name),
        )
        self.assertTrue(stage is not None)

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 0)

        with open(self.DATA_SUB, "a") as fd:
            fd.write("add")

        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc indeed registers changed output dir
        shutil.move(self.BAR, dir_name)
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        # Check that dvc registers mtime change for the directory.
        System.hardlink(self.DATA_SUB, self.DATA_SUB + ".lnk")
        stages = self.dvc.reproduce(stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)


class TestReproMissingMd5InStageFile(TestRepro):
    def test(self):
        d = load_stage_file(self.file1_stage)
        del d[Stage.PARAM_OUTS][0][RemoteLOCAL.PARAM_CHECKSUM]
        del d[Stage.PARAM_DEPS][0][RemoteLOCAL.PARAM_CHECKSUM]
        dump_stage_file(self.file1_stage, d)

        stages = self.dvc.reproduce(self.file1_stage)
        self.assertEqual(len(stages), 1)


class TestCmdRepro(TestReproChangedData):
    def test(self):
        self.swap_foo_with_bar()

        ret = main(["status"])
        self.assertEqual(ret, 0)

        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)

        ret = main(["repro", "non-existing-file"])
        self.assertNotEqual(ret, 0)


class TestCmdReproChdirCwdBackwardCompatible(TestDvc):
    def test(self):
        dname = "dir"
        os.mkdir(dname)
        foo = os.path.join(dname, self.FOO)
        bar = os.path.join(dname, self.BAR)
        code = os.path.join(dname, self.CODE)
        shutil.copyfile(self.FOO, foo)
        shutil.copyfile(self.CODE, code)

        ret = main(
            [
                "run",
                "-f",
                "Dvcfile",
                "-c",
                dname,
                "-d",
                self.FOO,
                "-o",
                self.BAR,
                "python {} {} {}".format(self.CODE, self.FOO, self.BAR),
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))

        os.unlink(bar)

        ret = main(["repro", "-c", dname])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))


class TestCmdReproChdir(TestDvc):
    def test(self):
        dname = "dir"
        os.mkdir(dname)
        foo = os.path.join(dname, self.FOO)
        bar = os.path.join(dname, self.BAR)
        code = os.path.join(dname, self.CODE)
        shutil.copyfile(self.FOO, foo)
        shutil.copyfile(self.CODE, code)

        ret = main(
            [
                "run",
                "-f",
                "{}/Dvcfile".format(dname),
                "-w",
                "{}".format(dname),
                "-d",
                self.FOO,
                "-o",
                self.BAR,
                "python {} {} {}".format(self.CODE, self.FOO, self.BAR),
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(foo))
        self.assertTrue(os.path.isfile(bar))
        self.assertTrue(filecmp.cmp(foo, bar, shallow=False))

        os.unlink(bar)

        ret = main(["repro", "-c", dname])
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
        return "://"

    @property
    def sep(self):
        return "/"

    def check_already_cached(self, stage):
        stage.outs[0].remove()

        patch_download = patch.object(
            stage.deps[0], "download", wraps=stage.deps[0].download
        )

        patch_checkout = patch.object(
            stage.outs[0], "checkout", wraps=stage.outs[0].checkout
        )

        patch_run = patch.object(stage, "_run", wraps=stage._run)

        with self.dvc.state:
            with patch_download as mock_download:
                with patch_checkout as mock_checkout:
                    with patch_run as mock_run:
                        stage.run()

                        mock_run.assert_not_called()
                        mock_download.assert_not_called()
                        mock_checkout.assert_called_once()

    @patch("dvc.prompt.confirm", return_value=True)
    def test(self, mock_prompt):
        if not self.should_test():
            return

        cache = (
            self.scheme
            + self.scheme_sep
            + self.bucket
            + self.sep
            + str(uuid.uuid4())
        )

        ret = main(["config", "cache." + self.cache_scheme, "myrepo"])
        self.assertEqual(ret, 0)
        ret = main(["remote", "add", "myrepo", cache])
        self.assertEqual(ret, 0)
        ret = main(["remote", "modify", "myrepo", "type", "hardlink"])
        self.assertEqual(ret, 0)

        remote_name = "myremote"
        remote_key = str(uuid.uuid4())
        remote = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + remote_key
        )

        ret = main(["remote", "add", remote_name, remote])
        self.assertEqual(ret, 0)
        ret = main(["remote", "modify", remote_name, "type", "hardlink"])
        self.assertEqual(ret, 0)

        self.dvc = DvcRepo(".")

        foo_key = remote_key + self.sep + self.FOO
        bar_key = remote_key + self.sep + self.BAR

        foo_path = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + foo_key
        )
        bar_path = (
            self.scheme + self.scheme_sep + self.bucket + self.sep + bar_key
        )

        # Using both plain and remote notation
        out_foo_path = "remote://" + remote_name + "/" + self.FOO
        out_bar_path = bar_path

        self.write(self.bucket, foo_key, self.FOO_CONTENTS)

        import_stage = self.dvc.imp(out_foo_path, "import")

        self.assertTrue(os.path.exists("import"))
        self.assertTrue(filecmp.cmp("import", self.FOO, shallow=False))
        self.assertEqual(self.dvc.status(import_stage.path), {})
        self.check_already_cached(import_stage)

        import_remote_stage = self.dvc.imp(
            out_foo_path, out_foo_path + "_imported"
        )
        self.assertEqual(self.dvc.status(import_remote_stage.path), {})

        cmd_stage = self.dvc.run(
            outs=[out_bar_path],
            deps=[out_foo_path],
            cmd=self.cmd(foo_path, bar_path),
        )

        self.assertEqual(self.dvc.status(cmd_stage.path), {})
        self.assertEqual(self.dvc.status(), {})
        self.check_already_cached(cmd_stage)

        self.write(self.bucket, foo_key, self.BAR_CONTENTS)

        self.assertNotEqual(self.dvc.status(), {})

        stages = self.dvc.reproduce(import_stage.path)
        self.assertEqual(len(stages), 1)
        self.assertTrue(os.path.exists("import"))
        self.assertTrue(filecmp.cmp("import", self.BAR, shallow=False))
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
        return "s3"

    @property
    def bucket(self):
        return TEST_AWS_REPO_BUCKET

    def cmd(self, i, o):
        return "aws s3 cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        s3 = boto3.resource("s3")
        s3.Bucket(bucket).put_object(Key=key, Body=body)


class TestReproExternalGS(TestReproExternalBase):
    def should_test(self):
        return _should_test_gcp()

    @property
    def scheme(self):
        return "gs"

    @property
    def bucket(self):
        return TEST_GCP_REPO_BUCKET

    def cmd(self, i, o):
        return "gsutil cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        client = gc.Client()
        bucket = client.bucket(bucket)
        bucket.blob(key).upload_from_string(body)


class TestReproExternalHDFS(TestReproExternalBase):
    def should_test(self):
        return _should_test_hdfs()

    @property
    def scheme(self):
        return "hdfs"

    @property
    def bucket(self):
        return "{}@127.0.0.1".format(getpass.getuser())

    def cmd(self, i, o):
        return "hadoop fs -cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        url = self.scheme + "://" + bucket + "/" + key
        p = Popen(
            "hadoop fs -rm -f {}".format(url),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        p.communicate()

        p = Popen(
            "hadoop fs -mkdir -p {}".format(posixpath.dirname(url)),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)

        with open("tmp", "w+") as fd:
            fd.write(body)

        p = Popen(
            "hadoop fs -copyFromLocal {} {}".format("tmp", url),
            shell=True,
            executable=os.getenv("SHELL"),
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            print(out)
            print(err)
        self.assertEqual(p.returncode, 0)


@flaky(max_runs=3, min_passes=1)
class TestReproExternalSSH(TestReproExternalBase):
    _dir = None

    def should_test(self):
        return _should_test_ssh()

    @property
    def scheme(self):
        return "ssh"

    @property
    def bucket(self):
        if not self._dir:
            self._dir = TestDvc.mkdtemp()
        return "{}@127.0.0.1:{}".format(getpass.getuser(), self._dir)

    def cmd(self, i, o):
        i = i.strip("ssh://")
        o = o.strip("ssh://")
        return "scp {} {}".format(i, o)

    def write(self, bucket, key, body):
        path = posixpath.join(self._dir, key)

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("127.0.0.1")

        sftp = ssh.open_sftp()
        try:
            sftp.stat(path)
            sftp.remove(path)
        except IOError:
            pass

        stdin, stdout, stderr = ssh.exec_command(
            "mkdir -p $(dirname {})".format(path)
        )
        self.assertEqual(stdout.channel.recv_exit_status(), 0)

        with sftp.open(path, "w+") as fobj:
            fobj.write(body)


class TestReproExternalLOCAL(TestReproExternalBase):
    def setUp(self):
        super(TestReproExternalLOCAL, self).setUp()
        self.tmpdir = TestDvc.mkdtemp()
        ret = main(["config", "cache.type", "hardlink"])
        self.assertEqual(ret, 0)
        self.dvc = DvcRepo(".")

    def should_test(self):
        return True

    @property
    def cache_scheme(self):
        return "local"

    @property
    def scheme(self):
        return ""

    @property
    def scheme_sep(self):
        return ""

    @property
    def sep(self):
        return os.sep

    @property
    def bucket(self):
        return self.tmpdir

    def cmd(self, i, o):
        if os.name == "nt":
            return "copy {} {}".format(i, o)
        return "cp {} {}".format(i, o)

    def write(self, bucket, key, body):
        path = os.path.join(bucket, key)
        dname = os.path.dirname(path)

        if not os.path.exists(dname):
            os.makedirs(dname)

        with open(path, "w+") as fd:
            fd.write(body)


class TestReproExternalHTTP(TestReproExternalBase):
    _external_cache_id = None

    @property
    def remote(self):
        return "http://localhost:8000/"

    @property
    def local_cache(self):
        return os.path.join(self.dvc.dvc_dir, "cache")

    @property
    def external_cache_id(self):
        if not self._external_cache_id:
            self._external_cache_id = str(uuid.uuid4())

        return self._external_cache_id

    @property
    def external_cache(self):
        return urljoin(self.remote, self.external_cache_id)

    def test(self):
        ret1 = main(["remote", "add", "mycache", self.external_cache])
        ret2 = main(["remote", "add", "myremote", self.remote])
        self.assertEqual(ret1, 0)
        self.assertEqual(ret2, 0)

        self.dvc = DvcRepo(".")

        # Import
        with StaticFileServer():
            import_url = urljoin(self.remote, self.FOO)
            import_output = "imported_file"
            import_stage = self.dvc.imp(import_url, import_output)

        self.assertTrue(os.path.exists(import_output))
        self.assertTrue(filecmp.cmp(import_output, self.FOO, shallow=False))

        self.dvc.remove("imported_file.dvc")

        with StaticFileServer(handler="Content-MD5"):
            import_url = urljoin(self.remote, self.FOO)
            import_output = "imported_file"
            import_stage = self.dvc.imp(import_url, import_output)

        self.assertTrue(os.path.exists(import_output))
        self.assertTrue(filecmp.cmp(import_output, self.FOO, shallow=False))

        # Run --deps
        with StaticFileServer():
            run_dependency = urljoin(self.remote, self.BAR)
            run_output = "remote_file"
            cmd = 'open("{}", "w+")'.format(run_output)

            with open("create-output.py", "w") as fd:
                fd.write(cmd)

            run_stage = self.dvc.run(
                deps=[run_dependency],
                outs=[run_output],
                cmd="python create-output.py",
            )
            self.assertTrue(run_stage is not None)

        self.assertTrue(os.path.exists(run_output))

        # Pull
        self.dvc.remove(import_stage.path, outs_only=True)
        self.assertFalse(os.path.exists(import_output))

        shutil.move(self.local_cache, self.external_cache_id)
        self.assertFalse(os.path.exists(self.local_cache))

        with StaticFileServer():
            self.dvc.pull(import_stage.path, remote="mycache")

        self.assertTrue(os.path.exists(import_output))


class TestReproShell(TestDvc):
    def test(self):
        if os.name == "nt":
            return

        fname = "shell.txt"
        stage = fname + ".dvc"

        self.dvc.run(
            fname=stage, outs=[fname], cmd="echo $SHELL > {}".format(fname)
        )

        with open(fname, "r") as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())

        os.unlink(fname)

        self.dvc.reproduce(stage)

        with open(fname, "r") as fd:
            self.assertEqual(os.getenv("SHELL"), fd.read().strip())


class TestReproNoSCM(TestRepro):
    def test(self):
        shutil.rmtree(self.dvc.scm.dir)
        ret = main(["repro", self.file1_stage])
        self.assertEqual(ret, 0)


class TestReproAllPipelines(TestDvc):
    def test(self):
        self.dvc.run(
            fname="start.dvc", outs=["start.txt"], cmd="echo start > start.txt"
        )

        self.dvc.run(
            fname="middle.dvc",
            deps=["start.txt"],
            outs=["middle.txt"],
            cmd="echo middle > middle.txt",
        )

        self.dvc.run(
            fname="final.dvc",
            deps=["middle.txt"],
            outs=["final.txt"],
            cmd="echo final > final.txt",
        )

        self.dvc.run(
            fname="disconnected.dvc",
            outs=["disconnected.txt"],
            cmd="echo other > disconnected.txt",
        )

        with patch.object(Stage, "reproduce") as mock_reproduce:
            ret = main(["repro", "--all-pipelines"])
            self.assertEqual(ret, 0)
            self.assertEqual(mock_reproduce.call_count, 4)


class TestReproNoCommit(TestRepro):
    def test(self):
        shutil.rmtree(self.dvc.cache.local.cache_dir)
        ret = main(["repro", self.file1_stage, "--no-commit"])
        self.assertEqual(ret, 0)
        self.assertEqual(len(os.listdir(self.dvc.cache.local.cache_dir)), 0)


class TestReproAlreadyCached(TestRepro):
    def test(self):
        run_out = self.dvc.run(
            fname="datetime.dvc",
            deps=[],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
        ).outs[0]

        repro_out = self.dvc.reproduce(target="datetime.dvc")[0].outs[0]

        self.assertNotEqual(run_out.checksum, repro_out.checksum)

    def test_force_with_dependencies(self):
        run_out = self.dvc.run(
            fname="datetime.dvc",
            deps=[self.FOO],
            outs=["datetime.txt"],
            cmd='python -c "import time; print(time.time())" > datetime.txt',
        ).outs[0]

        ret = main(["repro", "--force", "datetime.dvc"])
        self.assertEqual(ret, 0)

        repro_out = Stage.load(self.dvc, "datetime.dvc").outs[0]

        self.assertNotEqual(run_out.checksum, repro_out.checksum)

    def test_force_import(self):
        ret = main(["import", self.FOO, self.BAR])
        self.assertEqual(ret, 0)

        patch_download = patch.object(
            RemoteLOCAL,
            "download",
            side_effect=RemoteLOCAL.download,
            autospec=True,
        )

        patch_checkout = patch.object(
            OutputBase,
            "checkout",
            side_effect=OutputBase.checkout,
            autospec=True,
        )

        with patch_download as mock_download:
            with patch_checkout as mock_checkout:
                ret = main(["repro", "--force", "bar.dvc"])
                self.assertEqual(ret, 0)
                self.assertEqual(mock_download.call_count, 1)
                self.assertEqual(mock_checkout.call_count, 0)


class TestShouldDisplayMetricsOnReproWithMetricsOption(TestDvc):
    def test(self):
        metrics_file = "metrics_file"
        metrics_value = 0.123489015
        ret = main(
            [
                "run",
                "-m",
                metrics_file,
                "echo {} >> {}".format(metrics_value, metrics_file),
            ]
        )
        self.assertEqual(0, ret)

        self._caplog.clear()
        ret = main(
            [
                "repro",
                "--force",
                "--metrics",
                metrics_file + Stage.STAGE_FILE_SUFFIX,
            ]
        )
        self.assertEqual(0, ret)

        expected_metrics_display = "{}: {}".format(metrics_file, metrics_value)
        self.assertIn(expected_metrics_display, self._caplog.text)


@pytest.fixture
def foo_copy(repo_dir, dvc):
    stages = dvc.add(repo_dir.FOO)
    assert len(stages) == 1
    foo_stage = stages[0]
    assert foo_stage is not None

    fname = "foo_copy"
    stage_fname = fname + ".dvc"
    dvc.run(
        fname=stage_fname,
        outs=[fname],
        deps=[repo_dir.FOO, repo_dir.CODE],
        cmd="python {} {} {}".format(repo_dir.CODE, repo_dir.FOO, fname),
    )
    return {"fname": fname, "stage_fname": stage_fname}


def test_dvc_formatting_retained(dvc, foo_copy):
    root = Path(dvc.root_dir)
    stage_file = root / foo_copy["stage_fname"]

    # Add comments and custom formatting to stage file
    lines = list(map(_format_dvc_line, stage_file.read_text().splitlines()))
    lines.insert(0, "# Starting comment")
    stage_text = "".join(l + "\n" for l in lines)
    stage_file.write_text(stage_text)

    # Rewrite data source and repro
    (root / "foo").write_text("new_foo")
    dvc.reproduce(foo_copy["stage_fname"])

    # All differences should be only about md5
    assert _hide_md5(stage_text) == _hide_md5(stage_file.read_text())


def _format_dvc_line(line):
    # Add line comment for all cache and md5 keys
    if "cache:" in line or "md5:" in line:
        return line + " # line comment"
    # Format command as one word per line
    elif line.startswith("cmd: "):
        pre, command = line.split(None, 1)
        return pre + " >\n" + "\n".join("  " + s for s in command.split())
    else:
        return line


def _hide_md5(text):
    return re.sub(r"\b[a-f0-9]{32}\b", "<md5>", text)


class TestReproDownstream(TestDvc):
    def test(self):
        # The dependency graph should look like this:
        #
        #       E
        #      / \
        #     D   F
        #    / \   \
        #   B   C   G
        #    \ /
        #     A
        #
        assert main(["run", "-o", "A", "echo A>A"]) == 0
        assert main(["run", "-d", "A", "-o", "B", "echo B>B"]) == 0
        assert main(["run", "-d", "A", "-o", "C", "echo C>C"]) == 0
        assert main(["run", "-d", "B", "-d", "C", "-o", "D", "echo D>D"]) == 0
        assert main(["run", "-o", "G", "echo G>G"]) == 0
        assert main(["run", "-d", "G", "-o", "F", "echo F>F"]) == 0
        assert main(["run", "-d", "D", "-d", "F", "-o", "E", "echo E>E"]) == 0

        # We want the evaluation to move from B to E
        #
        #       E
        #      /
        #     D
        #    /
        #   B
        #
        evaluation = self.dvc.reproduce("B.dvc", downstream=True, force=True)

        assert len(evaluation) == 3
        assert evaluation[0].relpath == "B.dvc"
        assert evaluation[1].relpath == "D.dvc"
        assert evaluation[2].relpath == "E.dvc"
