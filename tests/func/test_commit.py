from dvc.utils.stage import load_stage_file, dump_stage_file

from tests.basic_env import TestDvc
from dvc.stage import StageCommitError


class TestCommitRecursive(TestDvc):
    def test(self):
        stages = self.dvc.add(self.DATA_DIR, recursive=True, no_commit=True)
        self.assertEqual(len(stages), 2)

        self.assertNotEqual(self.dvc.status(), {})

        self.dvc.commit(self.DATA_DIR, recursive=True)

        self.assertEqual(self.dvc.status(), {})


class TestCommitForce(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO, no_commit=True)
        self.assertEqual(len(stages), 1)
        stage = stages[0]

        with self.dvc.state:
            self.assertTrue(stage.outs[0].changed_cache())

        with open(self.FOO, "a") as fobj:
            fobj.write(self.FOO_CONTENTS)

        with self.dvc.state:
            self.assertTrue(stage.outs[0].changed_cache())

        with self.assertRaises(StageCommitError):
            self.dvc.commit(stage.path)

        with self.dvc.state:
            self.assertTrue(stage.outs[0].changed_cache())

        self.dvc.commit(stage.path, force=True)

        self.assertEqual(self.dvc.status(stage.path), {})


class TestCommitWithDeps(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO, no_commit=True)
        self.assertEqual(len(stages), 1)
        foo_stage = stages[0]
        self.assertTrue(foo_stage is not None)
        self.assertEqual(len(foo_stage.outs), 1)

        fname = "file"
        stage = self.dvc.run(
            cmd="python {} {} {}".format(self.CODE, self.FOO, fname),
            outs=[fname],
            deps=[self.FOO, self.CODE],
            no_commit=True,
        )
        self.assertTrue(stage is not None)
        self.assertEqual(len(stage.outs), 1)

        with self.dvc.state:
            self.assertTrue(foo_stage.outs[0].changed_cache())
            self.assertTrue(stage.outs[0].changed_cache())

        self.dvc.commit(stage.path, with_deps=True)
        with self.dvc.state:
            self.assertFalse(foo_stage.outs[0].changed_cache())
            self.assertFalse(stage.outs[0].changed_cache())


class TestCommitChangedMd5(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO, no_commit=True)
        self.assertEqual(len(stages), 1)
        stage = stages[0]

        st = load_stage_file(stage.path)
        st["md5"] = "1111111111"
        dump_stage_file(stage.path, st)

        with self.assertRaises(StageCommitError):
            self.dvc.commit(stage.path)

        self.dvc.commit(stage.path, force=True)
