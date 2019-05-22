import logging

from dvc.main import main

from tests.basic_env import TestDvc
from tests.func.test_repro import TestRepro, TestReproChangedDeepData
import os


class TestPipelineShowSingle(TestDvc):
    def setUp(self):
        super(TestPipelineShowSingle, self).setUp()
        self.stage = "foo.dvc"
        self.dotFile = "graph.dot"
        ret = main(["add", self.FOO])
        self.assertEqual(ret, 0)

    def test(self):
        ret = main(["pipeline", "show", self.stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(["pipeline", "show", self.stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_outs(self):
        ret = main(["pipeline", "show", self.stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_ascii(self):
        ret = main(["pipeline", "show", "--ascii", self.stage])
        self.assertEqual(ret, 0)

    def test_dot(self):
        ret = main(["pipeline", "show", "--dot", self.dotFile, self.stage])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_tree(self):
        ret = main(["pipeline", "show", "--tree", self.stage])
        self.assertEqual(ret, 0)

    def test_ascii_commands(self):
        ret = main(["pipeline", "show", "--ascii", self.stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(["pipeline", "show", "--ascii", self.stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_dot_commands(self):
        ret = main(
            [
                "pipeline",
                "show",
                "--dot",
                self.dotFile,
                self.stage,
                "--commands",
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_dot_outs(self):
        ret = main(
            ["pipeline", "show", "--dot", self.dotFile, self.stage, "--outs"]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_not_dvc_file(self):
        ret = main(["pipeline", "show", self.FOO])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(["pipeline", "show", "non-existing"])
        self.assertNotEqual(ret, 0)


class TestPipelineShow(TestRepro):
    def setUp(self):
        super(TestPipelineShow, self).setUp()
        self.dotFile = "graph.dot"

    def test(self):
        ret = main(["pipeline", "show", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(["pipeline", "show", self.file1_stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_outs(self):
        ret = main(["pipeline", "show", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_ascii(self):
        ret = main(["pipeline", "show", "--ascii", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_dot(self):
        ret = main(
            ["pipeline", "show", "--dot", self.dotFile, self.file1_stage]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_ascii_commands(self):
        ret = main(
            ["pipeline", "show", "--ascii", self.file1_stage, "--commands"]
        )
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(["pipeline", "show", "--ascii", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_dot_commands(self):
        ret = main(
            [
                "pipeline",
                "show",
                "--dot",
                self.dotFile,
                self.file1_stage,
                "--commands",
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_dot_outs(self):
        ret = main(
            [
                "pipeline",
                "show",
                "--dot",
                self.dotFile,
                self.file1_stage,
                "--outs",
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_not_dvc_file(self):
        ret = main(["pipeline", "show", self.file1])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(["pipeline", "show", "non-existing"])
        self.assertNotEqual(ret, 0)

    def test_print_locked_stages(self):
        self.dvc.add("foo")
        self.dvc.add("bar")
        self.dvc.lock_stage("foo.dvc")

        self._caplog.clear()
        with self._caplog.at_level(logging.INFO, logger="dvc"):
            ret = main(["pipeline", "show", "foo.dvc", "--locked"])
            self.assertEqual(ret, 0)

        assert "foo.dvc" in self._caplog.text
        assert "bar.dvc" not in self._caplog.text


class TestPipelineShowDeep(TestReproChangedDeepData):
    def setUp(self):
        super(TestPipelineShowDeep, self).setUp()
        self.dotFile = "graph.dot"

    def test(self):
        ret = main(["pipeline", "show", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(["pipeline", "show", self.file1_stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_outs(self):
        ret = main(["pipeline", "show", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_ascii(self):
        ret = main(["pipeline", "show", "--ascii", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_dot(self):
        ret = main(
            ["pipeline", "show", "--dot", self.dotFile, self.file1_stage]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_ascii_commands(self):
        ret = main(
            ["pipeline", "show", "--ascii", self.file1_stage, "--commands"]
        )
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(["pipeline", "show", "--ascii", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_dot_commands(self):
        ret = main(
            [
                "pipeline",
                "show",
                "--dot",
                self.dotFile,
                self.file1_stage,
                "--commands",
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_dot_outs(self):
        ret = main(
            [
                "pipeline",
                "show",
                "--dot",
                self.dotFile,
                self.file1_stage,
                "--outs",
            ]
        )
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.isfile(self.dotFile))

    def test_not_dvc_file(self):
        ret = main(["pipeline", "show", self.file1])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(["pipeline", "show", "non-existing"])
        self.assertNotEqual(ret, 0)


class TestPipelineListEmpty(TestDvc):
    def test(self):
        ret = main(["pipeline", "list"])
        self.assertEqual(ret, 0)


class TestPipelineListSingle(TestPipelineShowDeep):
    def test(self):
        ret = main(["pipeline", "list"])
        self.assertEqual(ret, 0)


class TestDvcRepoPipeline(TestDvc):
    def test_no_stages(self):
        pipelines = self.dvc.pipelines()
        self.assertEqual(len(pipelines), 0)

    def one_pipeline(self):
        self.dvc.add("foo")
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="")
        self.dvc.run(deps=["bar"], outs=["baz"], cmd="echo baz > baz")
        pipelines = self.dvc.pipelines()

        self.assertEqual(len(pipelines), 1)
        self.assertEqual(pipelines[0].nodes, 3)
        self.assertEqual(pipelines[0].edges, 2)

    def two_pipelines(self):
        self.dvc.add("foo")
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="")
        self.dvc.run(deps=["bar"], outs=["baz"], cmd="echo baz > baz")

        self.dvc.add("code.py")

        pipelines = self.dvc.pipelines()

        self.assertEqual(len(pipelines), 2)
        self.assertEqual(pipelines[0].nodes, 3)
        self.assertEqual(pipelines[0].edges, 2)
        self.assertEqual(pipelines[1].nodes, 1)
        self.assertEqual(pipelines[1].edges, 0)

    def locked_stage(self):
        self.dvc.add("foo")
        self.dvc.lock_stage("foo.dvc")

        pipelines = self.dvc.pipelines()
        self.assertEqual(len(pipelines), 0)
