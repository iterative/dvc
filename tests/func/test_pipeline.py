import logging

from dvc.main import main
from dvc.command.pipeline import CmdPipelineShow, CmdPipelineList
from tests.basic_env import TestDvc
from tests.func.test_repro import TestRepro
from tests.func.test_repro import TestReproChangedDeepData


class TestPipelineShowSingle(TestDvc):
    def setUp(self):
        super().setUp()
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


def test_single_ascii(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo content")
    assert main(["pipeline", "show", "--ascii", "foo.dvc"]) == 0


def test_single_ascii_commands(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo", "foo content")
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


def test_disconnected_stage(tmp_dir, dvc):
    tmp_dir.dvc_gen({"base": "base"})

    dvc.add("base")
    dvc.run(
        deps=["base"],
        outs=["derived1"],
        cmd="echo derived1 > derived1",
        single_stage=True,
    )
    dvc.run(
        deps=["base"],
        outs=["derived2"],
        cmd="echo derived2 > derived2",
        single_stage=True,
    )
    final_stage = dvc.run(
        deps=["derived1"],
        outs=["final"],
        cmd="echo final > final",
        single_stage=True,
    )

    command = CmdPipelineShow([])
    # Need to test __build_graph directly
    nodes, edges, is_tree = command._build_graph(
        final_stage.path, commands=False, outs=True
    )

    assert set(nodes) == {"final", "derived1", "base"}
    assert edges == [("final", "derived1"), ("derived1", "base")]
    assert is_tree is True


def test_print_locked_stages(tmp_dir, dvc, caplog):
    tmp_dir.dvc_gen({"foo": "foo content", "bar": "bar content"})
    dvc.lock_stage("foo.dvc")

    caplog.clear()
    with caplog.at_level(logging.INFO, logger="dvc"):
        assert main(["pipeline", "show", "foo.dvc", "--locked"]) == 0

    assert "foo.dvc" in caplog.text
    assert "bar.dvc" not in caplog.text


def test_dot_outs(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo content")
    run_copy("foo", "file", single_stage=True)
    assert main(["pipeline", "show", "--dot", "file.dvc", "--outs"]) == 0


class TestPipelineShowOuts(TestRepro):
    def setUp(self):
        super().setUp()

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
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="", single_stage=True)
        self.dvc.run(
            deps=["bar"], outs=["baz"], cmd="echo baz > baz", single_stage=True
        )
        pipelines = self.dvc.pipelines

        self.assertEqual(len(pipelines), 1)
        self.assertEqual(pipelines[0].nodes, 3)
        self.assertEqual(pipelines[0].edges, 2)

    def two_pipelines(self):
        self.dvc.add("foo")
        self.dvc.run(deps=["foo"], outs=["bar"], cmd="", single_stage=True)
        self.dvc.run(
            deps=["bar"], outs=["baz"], cmd="echo baz > baz", single_stage=True
        )

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


def test_split_pipeline(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("git_dep1", "git_dep1")
    tmp_dir.scm_gen("git_dep2", "git_dep2")

    tmp_dir.dvc_gen("data", "source file content")
    dvc.run(
        deps=["git_dep1", "data"],
        outs=["data_train", "data_valid"],
        cmd="echo train >> data_train && echo valid >> data_valid",
        single_stage=True,
    )
    stage = dvc.run(
        deps=["git_dep2", "data_train", "data_valid"],
        outs=["result"],
        cmd="echo result >> result",
        single_stage=True,
    )

    command = CmdPipelineShow([])
    nodes, edges, is_tree = command._build_graph(
        stage.path, commands=False, outs=True
    )
    assert set(nodes) == {"data", "data_train", "data_valid", "result"}
    assert set(edges) == {
        ("result", "data_train"),
        ("result", "data_valid"),
        ("data_train", "data"),
        ("data_valid", "data"),
    }


def test_pipeline_list_show_multistage(tmp_dir, dvc, run_copy, caplog):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "foobar", single_stage=True)
    command = CmdPipelineShow([])

    caplog.clear()
    with caplog.at_level(logging.INFO, "dvc"):
        command._show("foobar.dvc", False, False, False)
        output = caplog.text.splitlines()
        assert "dvc.yaml:copy-foo-bar" in output[0]
        assert "foobar.dvc" in output[1]

    caplog.clear()
    with caplog.at_level(logging.INFO, "dvc"):
        command._show("dvc.yaml:copy-foo-bar", False, False, False)
        assert "dvc.yaml:copy-foo-bar" in caplog.text
        assert "foobar.dvc" not in caplog.text

    command = CmdPipelineList([])
    caplog.clear()
    with caplog.at_level(logging.INFO, "dvc"):
        command.run()
        assert "dvc.yaml:copy-foo-bar" in caplog.text
        assert "foobar.dvc" in caplog.text
        assert "1 pipelines in total"


def test_pipeline_ascii_multistage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")
    run_copy("bar", "foobar", single_stage=True)
    command = CmdPipelineShow([])
    nodes, edges, is_tree = command._build_graph("foobar.dvc")
    assert set(nodes) == {"dvc.yaml:copy-foo-bar", "foobar.dvc"}
    assert set(edges) == {
        ("foobar.dvc", "dvc.yaml:copy-foo-bar"),
    }

    nodes, edges, is_tree = command._build_graph("dvc.yaml:copy-foo-bar")
    assert set(nodes) == {"dvc.yaml:copy-foo-bar"}
