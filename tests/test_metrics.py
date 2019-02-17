import os
import json

from dvc.repo import Repo as DvcRepo
from dvc.main import main
from dvc.exceptions import DvcException, BadMetricError, NoMetricsError
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

            files = [
                "metric",
                "metric_json",
                "metric_tsv",
                "metric_htsv",
                "metric_csv",
                "metric_hcsv",
            ]

            self.dvc.run(metrics_no_cache=files, overwrite=True)

            self.dvc.scm.add(files + ["metric.dvc"])

            self.dvc.scm.commit("metric")

        self.dvc.scm.checkout("master")

    def test(self):
        ret = self.dvc.metrics.show("metric", all_branches=True)
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric"] == "foo")
        self.assertTrue(ret["bar"]["metric"] == "bar")
        self.assertTrue(ret["baz"]["metric"] == "baz")

        ret = self.dvc.metrics.show(
            "metric_json", typ="json", xpath="branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_json"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_json"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_json"] == ["baz"])

        ret = self.dvc.metrics.show(
            "metric_tsv", typ="TsV", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_tsv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_tsv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_tsv"] == ["baz"])

        ret = self.dvc.metrics.show(
            "metric_htsv", typ="htsv", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_htsv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_htsv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_htsv"] == ["baz"])

        ret = self.dvc.metrics.show(
            "metric_csv", typ="CSV", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertTrue(ret["foo"]["metric_csv"] == ["foo"])
        self.assertTrue(ret["bar"]["metric_csv"] == ["bar"])
        self.assertTrue(ret["baz"]["metric_csv"] == ["baz"])

        ret = self.dvc.metrics.show(
            "metric_hcsv", typ="hcsv", xpath="0,branch", all_branches=True
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
        with self.assertRaises(BadMetricError):
            self.dvc.metrics.show("nested", all_branches=True, recursive=False)

        ret = self.dvc.metrics.show(
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

        ret = main(["metrics", "modify", "-t", "XSV", "-x", "0,0", "metrics"])
        self.assertEqual(ret, 1)

        ret = main(["metrics", "modify", "-t", "CSV", "-x", "0,0", "metrics"])
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
                "0,branch",
            ]
        )
        self.assertEqual(ret, 0)

        ret = main(
            ["metrics", "show", "-a", "metric_csv", "-t", "CSV", "-x", "0,0"]
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
                "0,branch",
            ]
        )
        self.assertEqual(ret, 0)

    def test_dir(self):
        os.mkdir("metrics_dir")

        with self.assertRaises(DvcException):
            self.dvc.run(outs_no_cache=["metrics_dir"])
            self.dvc.metrics.add("metrics_dir")

    def test_binary(self):
        with open("metrics_bin", "w+") as fd:
            fd.write("\0\0\0\0\0\0\0\0")

        with self.assertRaises(DvcException):
            self.dvc.run(outs_no_cache=["metrics_bin"])
            self.dvc.metrics.add("metrics_bin")

    def test_non_existing(self):
        ret = main(["metrics", "add", "non-existing"])
        self.assertNotEqual(ret, 0)

        ret = main(["metrics", "modify", "non-existing"])
        self.assertNotEqual(ret, 0)

        ret = main(["metrics", "remove", "non-existing"])
        self.assertNotEqual(ret, 0)


class TestNoMetrics(TestDvc):
    def test(self):
        with self.assertRaises(NoMetricsError):
            self.dvc.metrics.show()

    def test_cli(self):
        ret = main(["metrics", "show"])
        self.assertNotEqual(ret, 0)


class TestCachedMetrics(TestDvc):
    def _do_add(self, branch):
        self.dvc.scm.checkout(branch)
        self.dvc.checkout(force=True)

        with open("metrics.json", "w+") as fd:
            json.dump({"metrics": branch}, fd)

        stages = self.dvc.add("metrics.json")
        self.dvc.metrics.add("metrics.json", typ="json", xpath="metrics")
        self.assertEqual(len(stages), 1)
        stage = stages[0]
        self.assertIsNotNone(stage)

        self.dvc.scm.add([".gitignore", "metrics.json.dvc"])
        self.dvc.scm.commit(branch)

    def _do_run(self, branch):
        self.dvc.scm.checkout(branch)
        self.dvc.checkout(force=True)

        with open("code.py", "w+") as fobj:
            fobj.write("import sys\n")
            fobj.write("import os\n")
            fobj.write("import json\n")
            fobj.write(
                'print(json.dumps({{"metrics": "{branch}"}}))\n'.format(
                    branch=branch
                )
            )

        stage = self.dvc.run(
            deps=["code.py"],
            metrics=["metrics.json"],
            cmd="python code.py metrics.json > metrics.json",
        )
        self.assertIsNotNone(stage)
        self.assertEqual(stage.relpath, "metrics.json.dvc")

        self.dvc.scm.add([".gitignore", "metrics.json.dvc"])
        self.dvc.scm.commit(branch)

    def _test_metrics(self, func):
        self.dvc.scm.commit("init")

        self.dvc.scm.branch("one")
        self.dvc.scm.branch("two")

        func("master")
        func("one")
        func("two")

        self.dvc = DvcRepo(".")

        res = self.dvc.metrics.show(
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

        res = self.dvc.metrics.show(
            "", all_branches=True, typ="json", xpath="metrics"
        )

        self.assertEqual(
            res,
            {
                "master": {"metrics.json": ["master"]},
                "one": {"metrics.json": ["one"]},
                "two": {"metrics.json": ["two"]},
            },
        )

    def test_add(self):
        self._test_metrics(self._do_add)

    def test_run(self):
        self._test_metrics(self._do_run)
