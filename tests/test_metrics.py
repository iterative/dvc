import os
import json

from dvc.repo import Repo as DvcRepo
from dvc.main import main
from dvc.exceptions import DvcException, BadMetricError, NoMetricsError
from tests.basic_env import TestDvc

import dvc.logger as logger
from tests.utils import reset_logger_error_output, reset_logger_standard_output
from tests.utils.logger import MockLoggerHandlers


class TestMetrics(TestDvc):
    def setUp(self):
        super(TestMetrics, self).setUp()
        self.dvc.scm.commit("init")

        for branch in ["foo", "bar", "baz"]:
            self.dvc.scm.checkout(branch, create_new=True)

            with open("metric", "w+") as fd:
                fd.write(branch)

            with open("metric.json", "w+") as fd:
                json.dump({"branch": branch}, fd)

            with open("metric.csv", "w+") as fd:
                fd.write(branch)

            with open("metric.hcsv", "w+") as fd:
                fd.write("branch\n")
                fd.write(branch)

            with open("metric.tsv", "w+") as fd:
                fd.write(branch)

            with open("metric.htsv", "w+") as fd:
                fd.write("branch\n")
                fd.write(branch)

            files = [
                "metric",
                "metric.json",
                "metric.tsv",
                "metric.htsv",
                "metric.csv",
                "metric.hcsv",
            ]

            self.dvc.run(metrics_no_cache=files, overwrite=True)

            self.dvc.scm.add(files + ["metric.dvc"])

            self.dvc.scm.commit("metric")

        self.dvc.scm.checkout("master")

    def test_show(self):
        ret = self.dvc.metrics.show("metric", all_branches=True)
        self.assertEqual(len(ret), 3)
        self.assertEqual(ret["foo"]["metric"], "foo")
        self.assertEqual(ret["bar"]["metric"], "bar")
        self.assertEqual(ret["baz"]["metric"], "baz")

        ret = self.dvc.metrics.show(
            "metric.json", xpath="branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric.json"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric.json"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric.json"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric.tsv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric.tsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric.tsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric.tsv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric.htsv", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric.htsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric.htsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric.htsv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric.csv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric.csv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric.csv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric.csv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric.hcsv", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric.hcsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric.hcsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric.hcsv"], ["baz"])

    def test_unknown_type_ignored(self):
        ret = self.dvc.metrics.show(
            "metric.hcsv", typ="unknown", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric.hcsv"].split(), ["branch", b])

    def test_type_case_normalized(self):
        ret = self.dvc.metrics.show(
            "metric.hcsv", typ=" hCSV ", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric.hcsv"], [b])

    def test_xpath_is_empty(self):
        ret = self.dvc.metrics.show("metric.json", xpath="", all_branches=True)
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric.json"], json.dumps({"branch": b}))

    def test_xpath_is_none(self):
        ret = self.dvc.metrics.show(
            "metric.json", xpath=None, all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric.json"], json.dumps({"branch": b}))

    def test_xpath_all_columns(self):
        ret = self.dvc.metrics.show(
            "metric.hcsv", xpath="0,", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric.hcsv"], [b])

    def test_xpath_all_rows(self):
        ret = self.dvc.metrics.show(
            "metric.csv", xpath=",0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric.csv"], [b])

    def test_xpath_all(self):
        ret = self.dvc.metrics.show("metric.csv", xpath=",", all_branches=True)
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric.csv"], [[b]])

    def test_xpath_all_with_header(self):
        ret = self.dvc.metrics.show(
            "metric.hcsv", xpath=",", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric.hcsv"], [[b]])


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

        self.dvc.scm.add(
            ["nested", "metric_nested.dvc", "metric_subnested.dvc"]
        )
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

        ret = main(["metrics", "show", "-a", "metric.json", "-x", "branch"])
        self.assertEqual(ret, 0)
        ret = main(["metrics", "show", "-a", "metric.tsv", "-x", "0,0"])
        self.assertEqual(ret, 0)
        ret = main(["metrics", "show", "-a", "metric.htsv", "-x", "0,branch"])
        self.assertEqual(ret, 0)

        ret = main(["metrics", "show", "-a", "metric.csv", "-x", "0,0"])
        self.assertEqual(ret, 0)

        ret = main(["metrics", "show", "-a", "metric.hcsv", "-x", "0,branch"])
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

    def test_wrong_type_add(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["add", "metric.unknown"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger.logger):
            reset_logger_error_output()
            ret = main(["metrics", "add", "metric.unknown", "-t", "unknown"])
            self.assertEqual(ret, 1)
            self.assertIn(
                "failed to add metric file 'metric.unknown' - metric type "
                "'unknown' is not supported, must be one of "
                "[raw, json, csv, tsv, hcsv, htsv]",
                logger.logger.handlers[1].stream.getvalue(),
            )

            ret = main(["metrics", "add", "metric.unknown", "-t", "raw"])
            self.assertEqual(ret, 0)

            reset_logger_standard_output()
            ret = main(["metrics", "show", "metric.unknown"])
            self.assertEqual(ret, 0)
            self.assertIn(
                "\tmetric.unknown: unknown",
                logger.logger.handlers[0].stream.getvalue(),
            )

    def test_wrong_type_modify(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["run", "-m", "metric.unknown"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger.logger):
            reset_logger_error_output()
            ret = main(
                ["metrics", "modify", "metric.unknown", "-t", "unknown"]
            )
            self.assertEqual(ret, 1)
            self.assertIn(
                "failed to modify metric file settings - metric type "
                "'unknown' is not supported, must be one of "
                "[raw, json, csv, tsv, hcsv, htsv]",
                logger.logger.handlers[1].stream.getvalue(),
            )

            ret = main(["metrics", "modify", "metric.unknown", "-t", "CSV"])
            self.assertEqual(ret, 0)

            reset_logger_standard_output()
            ret = main(["metrics", "show", "metric.unknown"])
            self.assertEqual(ret, 0)
            self.assertIn(
                "\tmetric.unknown: unknown",
                logger.logger.handlers[0].stream.getvalue(),
            )

    def test_wrong_type_show(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["run", "-m", "metric.unknown"])
        self.assertEqual(ret, 0)

        with MockLoggerHandlers(logger.logger):
            reset_logger_standard_output()
            ret = main(
                [
                    "metrics",
                    "show",
                    "metric.unknown",
                    "-t",
                    "unknown",
                    "-x",
                    "0,0",
                ]
            )
            self.assertEqual(ret, 0)
            self.assertIn(
                "\tmetric.unknown: unknown",
                logger.logger.handlers[0].stream.getvalue(),
            )


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

        self.dvc.scm.add(["code.py", ".gitignore", "metrics.json.dvc"])
        self.dvc.scm.commit(branch)

    def _test_metrics(self, func):
        self.dvc.scm.commit("init")

        self.dvc.scm.branch("one")
        self.dvc.scm.branch("two")

        func("master")
        func("one")
        func("two")

        # TestDvc currently is based on TestGit, so it is safe to use
        # scm.git for now
        self.dvc.scm.git.git.clean("-fd")

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
