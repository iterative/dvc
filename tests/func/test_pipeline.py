import logging

from dvc.main import main
from tests.basic_env import TestDvc
from tests.func.test_repro import TestRepro
from tests.func.test_repro import TestReproChangedDeepData


class TestPipelineShowSingle(TestDvc):
    def setUp(self):
        super(TestPipelineShowSingle, self).setUp()
        self.stage = "foo.dvc"
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

    def test_dot(self):
        ret = main(["pipeline", "show", "--dot", self.stage])
        self.assertEqual(ret, 0)

    def test_tree(self):
        ret = main(["pipeline", "show", "--tree", self.stage])
        self.assertEqual(ret, 0)

    def test_ascii_outs(self):
        ret = main(["pipeline", "show", "--ascii", self.stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_dot_commands(self):
        ret = main(["pipeline", "show", "--dot", self.stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_dot_outs(self):
        ret = main(["pipeline", "show", "--dot", self.stage, "--outs"])
        self.assertEqual(ret, 0)

    def test_not_dvc_file(self):
        ret = main(["pipeline", "show", self.FOO])
        self.assertNotEqual(ret, 0)

    def test_non_existing(self):
        ret = main(["pipeline", "show", "non-existing"])
        self.assertNotEqual(ret, 0)


def test_single_ascii(repo_dir, dvc_repo):
    dvc_repo.add(repo_dir.FOO)
    assert main(["pipeline", "show", "--ascii", "foo.dvc"]) == 0


def test_single_ascii_commands(repo_dir, dvc_repo):
    dvc_repo.add(repo_dir.FOO)
    assert main(["pipeline", "show", "--ascii", "foo.dvc", "--commands"]) == 0


class TestPipelineShow(TestRepro):
    def test(self):
        ret = main(["pipeline", "show", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_commands(self):
        ret = main(["pipeline", "show", self.file1_stage, "--commands"])
        self.assertEqual(ret, 0)

    def test_ascii(self):
        ret = main(["pipeline", "show", "--ascii", self.file1_stage])
        self.assertEqual(ret, 0)

    def test_dot(self):
        ret = main(["pipeline", "show", "--dot", self.file1_stage])
        self.assertEqual(ret, 0)

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
            ["pipeline", "show", "--dot", self.file1_stage, "--commands"]
        )
        self.assertEqual(ret, 0)


def test_print_locked_stages(repo_dir, dvc_repo, caplog):
    dvc_repo.add("foo")
    dvc_repo.add("bar")
    dvc_repo.lock_stage("foo.dvc")

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc"):
        assert main(["pipeline", "show", "foo.dvc", "--locked"]) == 0

    assert "foo.dvc" in caplog.text
    assert "bar.dvc" not in caplog.text


def test_dot_outs(repo_dir, dvc_repo):
    dvc_repo.add(repo_dir.FOO)
    dvc_repo.run(
        outs=["file"],
        deps=[repo_dir.FOO, repo_dir.CODE],
        cmd="python {} {} {}".format(repo_dir.CODE, repo_dir.FOO, "file"),
    )
    assert main(["pipeline", "show", "--dot", "file.dvc", "--outs"]) == 0


class TestPipelineShowOuts(TestRepro):
    def setUp(self):
        super(TestPipelineShowOuts, self).setUp()

    def test_outs(self):
        ret = main(["pipeline", "show", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)


class TestPipelineShowDeep(TestReproChangedDeepData):
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
        ret = main(["pipeline", "show", "--dot", self.file1_stage])
        self.assertEqual(ret, 0)

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
            ["pipeline", "show", "--dot", self.file1_stage, "--commands"]
        )
        self.assertEqual(ret, 0)

    def test_dot_outs(self):
        ret = main(["pipeline", "show", "--dot", self.file1_stage, "--outs"])
        self.assertEqual(ret, 0)


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
        pipelines = self.dvc.pipelines
        self.assertEqual(len(pipelines), 0)

    def one_pipeline(self):
        self.dvc.add("foo")
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="")
        self.dvc.run(deps=["bar"], outs=["baz"], cmd="echo baz > baz")
        pipelines = self.dvc.pipelines

        self.assertEqual(len(pipelines), 1)
        self.assertEqual(pipelines[0].nodes, 3)
        self.assertEqual(pipelines[0].edges, 2)

    def two_pipelines(self):
        self.dvc.add("foo")
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="")
        self.dvc.run(deps=["bar"], outs=["baz"], cmd="echo baz > baz")

        self.dvc.add("code.py")

        pipelines = self.dvc.pipelines

        self.assertEqual(len(pipelines), 2)
        self.assertEqual(pipelines[0].nodes, 3)
        self.assertEqual(pipelines[0].edges, 2)
        self.assertEqual(pipelines[1].nodes, 1)
        self.assertEqual(pipelines[1].edges, 0)

    def locked_stage(self):
        self.dvc.add("foo")
        self.dvc.lock_stage("foo.dvc")

        pipelines = self.dvc.pipelines
        self.assertEqual(len(pipelines), 0)
