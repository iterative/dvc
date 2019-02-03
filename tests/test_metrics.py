import os
import json

from dvc.project import Project
from dvc.main import main
from dvc.exceptions import DvcException
from tests.basic_env import TestDvc


class TestMetrics(TestDvc):
    def setUp(self):
        super(TestMetrics, self).setUp()
        self.dvc.scm.commit("init")

        for branch in ["foo", "bar", "baz"]:
            self.dvc.scm.checkout(branch, create_new=True)

            with open("metric", "w+") as fd:
                fd.write(branch)

            with open("metric_json", "w+") as fd:
                json.dump({"branch": branch}, fd)

            with open("metric_csv", "w+") as fd:
                fd.write(branch)

            with open("metric_hcsv", "w+") as fd:
                fd.write("branch\n")
                fd.write(branch)

            with open("metric_tsv", "w+") as fd:
                fd.write(branch)

            with open("metric_htsv", "w+") as fd:
                fd.write("branch\n")
                fd.write(branch)

            self.dvc.scm.add(
                [
                    "metric",
                    "metric_json",
                    "metric_tsv",
                    "metric_htsv",
                    "metric_csv",
                    "metric_hcsv",
                ]
            )
            self.dvc.scm.commit("metric")

        self.dvc.scm.checkout("master")

    def test(self):
        ret = self.dvc.metrics_show("metric", all_branches=True)
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric"] == "foo")
        self.assertTrue(ret["bar"]["metric"] == "bar")
        self.assertTrue(ret["baz"]["metric"] == "baz")

        ret = self.dvc.metrics_show(
            "metric_json", typ="json", xpath="branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_json"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_json"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_json"] == ["baz"])

        ret = self.dvc.metrics_show(
            "metric_tsv", typ="tsv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_tsv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_tsv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_tsv"] == ["baz"])

        ret = self.dvc.metrics_show(
            "metric_htsv", typ="htsv", xpath="branch,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_htsv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_htsv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_htsv"] == ["baz"])

        ret = self.dvc.metrics_show(
            "metric_csv", typ="csv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_csv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_csv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_csv"] == ["baz"])

        ret = self.dvc.metrics_show(
            "metric_hcsv", typ="hcsv", xpath="branch,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_hcsv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_hcsv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_hcsv"] == ["baz"])


class TestMetricsRecursive(TestDvc):
    def setUp(self):
        super(TestMetricsRecursive, self).setUp()
        self.dvc.scm.commit("init")

        self.dvc.scm.checkout("nested", create_new=True)

        os.mkdir("nested")
        os.mkdir(os.path.join("nested", "subnested"))

        ret = main(
            [
                "run",
                "-M",
                os.path.join("nested", "metric_nested"),
                "echo",
                "nested",
                ">>",
                os.path.join("nested", "metric_nested"),
            ]
        )

        self.assertEqual(ret, 0)

        ret = main(
            [
                "run",
                "-M",
                os.path.join("nested", "subnested", "metric_subnested"),
                "echo",
                "subnested",
                ">>",
                os.path.join("nested", "subnested", "metric_subnested"),
            ]
        )

        self.assertEqual(ret, 0)

        self.dvc.scm.add(["nested"])
        self.dvc.scm.commit("nested metrics")

        self.dvc.scm.checkout("master")

    def test(self):

        ret = self.dvc.metrics_show(
            "nested", all_branches=True, recursive=False
        )
        self.assertEqual(len(ret), 0)

        ret = self.dvc.metrics_show(
            "nested", all_branches=True, recursive=True
        )
        self.assertEqual(len(ret), 1)
        self.assertEqual(
            ret["nested"][
                os.path.join("nested", "subnested", "metric_subnested")
            ],
            "subnested",
        )
        self.assertEqual(
            ret["nested"][os.path.join("nested", "metric_nested")], "nested"
        )


class TestMetricsReproCLI(TestDvc):
    def test(self):
        stage = self.dvc.run(
            metrics_no_cache=["metrics"],
            cmd="python {} {} {}".format(self.CODE, self.FOO, "metrics"),
        )

        ret = main(["repro", "-m", stage.path])
        self.assertEqual(ret, 0)

        ret = main(["metrics", "remove", "metrics"])
        self.assertEqual(ret, 0)

        ret = main(["repro", "-f", "-m", stage.path])
        self.assertNotEqual(ret, 0)

        ret = main(["metrics", "add", "metrics"])
        self.assertEqual(ret, 0)

        ret = main(["metrics", "modify", "-t", "csv", "-x", "0,0", "metrics"])
        self.assertEqual(ret, 0)

        ret = main(["repro", "-f", "-m", stage.path])
        self.assertEqual(ret, 0)

    def test_dir(self):
        os.mkdir("metrics_dir")

        with self.assertRaises(DvcException):
            self.dvc.run(metrics_no_cache=["metrics_dir"])

    def test_binary(self):
        with open("metrics_bin", "w+") as fd:
            fd.write("\0\0\0\0\0\0\0\0")

        with self.assertRaises(DvcException):
            self.dvc.run(metrics_no_cache=["metrics_bin"])


class TestMetricsCLICompat(TestMetrics):
    def test(self):
        # FIXME check output
        ret = main(["metrics", "show", "--all-branches", "metric", "-v"])
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "--all-branches",
                "metric_json",
                "--json-path",
                "branch",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "--all-branches",
                "metric_tsv",
                "--tsv-path",
                "0,0",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "--all-branches",
                "metric_htsv",
                "--htsv-path",
                "branch,0",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "--all-branches",
                "metric_csv",
                "--csv-path",
                "0,0",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "--all-branches",
                "metric_hcsv",
                "--hcsv-path",
                "branch,0",
            ]
        )
        self.assertEqual(ret, 0)


class TestMetricsCLI(TestMetrics):
    def test(self):
        # FIXME check output
        ret = main(["metrics", "show", "-a", "metric", "-v"])
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "-a",
                "metric_json",
                "-t",
                "json",
                "-x",
                "branch",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            ["metrics", "show", "-a", "metric_tsv", "-t", "tsv", "-x", "0,0"]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "-a",
                "metric_htsv",
                "-t",
                "htsv",
                "-x",
                "branch,0",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            ["metrics", "show", "-a", "metric_csv", "-t", "csv", "-x", "0,0"]
        )
        self.assertEqual(ret, 0)

        ret = main(
            [
                "metrics",
                "show",
                "-a",
                "metric_hcsv",
                "-t",
                "hcsv",
                "-x",
                "branch,0",
            ]
        )
        self.assertEqual(ret, 0)

    def test_dir(self):
        os.mkdir("metrics_dir")

        with self.assertRaises(DvcException):
            self.dvc.run(outs_no_cache=["metrics_dir"])
            self.dvc.metrics_add("metrics_dir")

    def test_binary(self):
        with open("metrics_bin", "w+") as fd:
            fd.write("\0\0\0\0\0\0\0\0")

        with self.assertRaises(DvcException):
            self.dvc.run(outs_no_cache=["metrics_bin"])
            self.dvc.metrics_add("metrics_bin")

    def test_non_existing(self):
        ret = main(["metrics", "add", "non-existing"])
        self.assertNotEqual(ret, 0)

        ret = main(["metrics", "modify", "non-existing"])
        self.assertNotEqual(ret, 0)

        ret = main(["metrics", "remove", "non-existing"])
        self.assertNotEqual(ret, 0)


class TestNoMetrics(TestDvc):
    def test(self):
        ret = main(["metrics", "show"])
        self.assertNotEqual(ret, 0)


class TestCachedMetrics(TestDvc):
    def _do_write(self, branch):
        self.dvc.scm.checkout(branch)
        self.dvc.checkout(force=True)

        with open("metrics.json", "w+") as fd:
            json.dump({"metrics": branch}, fd)

        stages = self.dvc.add("metrics.json")
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertNotEqual(stage, None)

        self.dvc.scm.add([".gitignore", "metrics.json.dvc"])
        self.dvc.scm.commit(branch)

    def test(self):
        self.dvc.scm.commit("init")

        self.dvc.scm.branch("one")
        self.dvc.scm.branch("two")

        self._do_write("master")
        self._do_write("one")
        self._do_write("two")

        self.dvc = Project(".")

        res = self.dvc.metrics_show(
            "metrics.json", all_branches=True, typ="json", xpath="metrics"
        )

        self.assertEqual(
            res,
            {
                "master": {"metrics.json": ["master"]},
                "one": {"metrics.json": ["one"]},
                "two": {"metrics.json": ["two"]},
            },
        )
