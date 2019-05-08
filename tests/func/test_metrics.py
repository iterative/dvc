# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os
import json
import logging

from dvc.repo import Repo as DvcRepo
from dvc.main import main
from dvc.exceptions import DvcException, BadMetricError, NoMetricsError
from dvc.repo.metrics.show import NO_METRICS_FILE_AT_REFERENCE_WARNING
from dvc.stage import Stage
from tests.basic_env import TestDvc


class TestMetricsBase(TestDvc):
    def setUp(self):
        super(TestMetricsBase, self).setUp()
        self.dvc.scm.commit("init")

        branches = ["foo", "bar", "baz"]

        for branch in branches:
            self.dvc.scm.git.create_head(branch)

        for branch in branches:
            self.dvc.scm.checkout(branch)

            self.create("metric", branch)
            self.create("metric_json", json.dumps({"branch": branch}))
            self.create("metric_csv", branch)
            self.create("metric_hcsv", "branch\n" + branch)
            self.create("metric_tsv", branch)
            self.create("metric_htsv", "branch\n" + branch)

            if branch == "foo":
                deviation_mse_train = 0.173461
            else:
                deviation_mse_train = 0.356245

            self.create(
                "metric_json_ext",
                json.dumps(
                    {
                        "metrics": [
                            {
                                "dataset": "train",
                                "deviation_mse": deviation_mse_train,
                                "value_mse": 0.421601,
                            },
                            {
                                "dataset": "testing",
                                "deviation_mse": 0.289545,
                                "value_mse": 0.297848,
                            },
                            {
                                "dataset": "validation",
                                "deviation_mse": 0.67528,
                                "value_mse": 0.671502,
                            },
                        ]
                    }
                ),
            )

            files = [
                "metric",
                "metric_json",
                "metric_tsv",
                "metric_htsv",
                "metric_csv",
                "metric_hcsv",
                "metric_json_ext",
            ]

            self.dvc.run(metrics_no_cache=files, overwrite=True)

            self.dvc.scm.add(files + ["metric.dvc"])

            self.dvc.scm.commit("metric")

        self.dvc.scm.checkout("master")


class TestMetrics(TestMetricsBase):
    def test_show(self):
        ret = self.dvc.metrics.show("metric", all_branches=True)
        self.assertEqual(len(ret), 3)
        self.assertEqual(ret["foo"]["metric"], "foo")
        self.assertEqual(ret["bar"]["metric"], "bar")
        self.assertEqual(ret["baz"]["metric"], "baz")

        ret = self.dvc.metrics.show(
            "metric_json", typ="json", xpath="branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric_json"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric_json"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric_json"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric_tsv", typ="tsv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric_tsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric_tsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric_tsv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric_htsv", typ="htsv", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric_htsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric_htsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric_htsv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric_csv", typ="csv", xpath="0,0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric_csv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric_csv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric_csv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric_hcsv", typ="hcsv", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        self.assertSequenceEqual(ret["foo"]["metric_hcsv"], ["foo"])
        self.assertSequenceEqual(ret["bar"]["metric_hcsv"], ["bar"])
        self.assertSequenceEqual(ret["baz"]["metric_hcsv"], ["baz"])

        ret = self.dvc.metrics.show(
            "metric_json_ext",
            typ="json",
            xpath="$.metrics[?(@.deviation_mse<0.30) & (@.value_mse>0.4)]",
            all_branches=True,
        )
        self.assertEqual(len(ret), 1)
        self.assertSequenceEqual(
            ret["foo"]["metric_json_ext"],
            [
                {
                    "dataset": "train",
                    "deviation_mse": 0.173461,
                    "value_mse": 0.421601,
                }
            ],
        )
        self.assertRaises(KeyError, lambda: ret["bar"])
        self.assertRaises(KeyError, lambda: ret["baz"])

    def test_unknown_type_ignored(self):
        ret = self.dvc.metrics.show(
            "metric_hcsv", typ="unknown", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric_hcsv"].split(), ["branch", b])

    def test_type_case_normalized(self):
        ret = self.dvc.metrics.show(
            "metric_hcsv", typ=" hCSV ", xpath="0,branch", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric_hcsv"], [b])

    def test_xpath_is_empty(self):
        ret = self.dvc.metrics.show(
            "metric_json", typ="json", xpath="", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric_json"], json.dumps({"branch": b}))

    def test_xpath_is_none(self):
        ret = self.dvc.metrics.show(
            "metric_json", typ="json", xpath=None, all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertEqual(ret[b]["metric_json"], json.dumps({"branch": b}))

    def test_xpath_all_columns(self):
        ret = self.dvc.metrics.show(
            "metric_hcsv", typ="hcsv ", xpath="0,", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric_hcsv"], [b])

    def test_xpath_all_rows(self):
        ret = self.dvc.metrics.show(
            "metric_csv", typ="csv", xpath=",0", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric_csv"], [b])

    def test_xpath_all(self):
        ret = self.dvc.metrics.show(
            "metric_csv", typ="csv", xpath=",", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric_csv"], [[b]])

    def test_xpath_all_with_header(self):
        ret = self.dvc.metrics.show(
            "metric_hcsv", typ="hcsv", xpath=",", all_branches=True
        )
        self.assertEqual(len(ret), 3)
        for b in ["foo", "bar", "baz"]:
            self.assertSequenceEqual(ret[b]["metric_hcsv"], [[b]])

    def test_formatted_output(self):
        # Labels are in Spanish to test UTF-8
        self.create(
            "metrics.csv",
            (
                "valor_mse,desviación_mse,data_set\n"
                "0.421601,0.173461,entrenamiento\n"
                "0.67528,0.289545,pruebas\n"
                "0.671502,0.297848,validación\n"
            ),
        )

        # Contains quoted newlines to test output correctness
        self.create(
            "metrics.tsv",
            (
                "value_mse\tdeviation_mse\tdata_set\n"
                "0.421601\t0.173461\ttrain\n"
                '0.67528\t0.289545\t"test\\ning"\n'
                "0.671502\t0.297848\tvalidation\n"
            ),
        )

        self.create(
            "metrics.json",
            (
                "{\n"
                '     "data_set": [\n'
                '          "train",\n'
                '          "testing",\n'
                '          "validation"\n'
                "     ],\n"
                '     "deviation_mse": [\n'
                '          "0.173461",\n'
                '          "0.289545",\n'
                '          "0.297848"\n'
                "     ],\n"
                '     "value_mse": [\n'
                '          "0.421601",\n'
                '          "0.67528",\n'
                '          "0.671502"\n'
                "     ]\n"
                "}"
            ),
        )

        self.create(
            "metrics.txt", "ROC_AUC: 0.64\nKS: 78.9999999996\nF_SCORE: 77\n"
        )

        self.dvc.run(
            fname="testing_metrics_output.dvc",
            metrics_no_cache=[
                "metrics.csv",
                "metrics.tsv",
                "metrics.json",
                "metrics.txt",
            ],
        )

        self.dvc.metrics.modify("metrics.csv", typ="csv")
        self.dvc.metrics.modify("metrics.tsv", typ="tsv")
        self.dvc.metrics.modify("metrics.json", typ="json")

        self._caplog.clear()

        with self._caplog.at_level(logging.INFO, logger="dvc"):
            ret = main(["metrics", "show"])
            self.assertEqual(ret, 0)

        expected_csv = (
            "\tmetrics.csv:\n"
            "\t\tvalor_mse   desviación_mse   data_set       \n"
            "\t\t0.421601    0.173461         entrenamiento  \n"
            "\t\t0.67528     0.289545         pruebas        \n"
            "\t\t0.671502    0.297848         validación"
        )

        expected_tsv = (
            "\tmetrics.tsv:\n"
            "\t\tvalue_mse   deviation_mse   data_set    \n"
            "\t\t0.421601    0.173461        train       \n"
            "\t\t0.67528     0.289545        test\\ning   \n"
            "\t\t0.671502    0.297848        validation"
        )

        expected_txt = (
            "\tmetrics.txt:\n"
            "\t\tROC_AUC: 0.64\n"
            "\t\tKS: 78.9999999996\n"
            "\t\tF_SCORE: 77"
        )

        expected_json = (
            "\tmetrics.json:\n"
            "\t\t{\n"
            '\t\t     "data_set": [\n'
            '\t\t          "train",\n'
            '\t\t          "testing",\n'
            '\t\t          "validation"\n'
            "\t\t     ],\n"
            '\t\t     "deviation_mse": [\n'
            '\t\t          "0.173461",\n'
            '\t\t          "0.289545",\n'
            '\t\t          "0.297848"\n'
            "\t\t     ],\n"
            '\t\t     "value_mse": [\n'
            '\t\t          "0.421601",\n'
            '\t\t          "0.67528",\n'
            '\t\t          "0.671502"\n'
            "\t\t     ]\n"
            "\t\t}"
        )

        stdout = "\n".join(record.message for record in self._caplog.records)

        assert expected_tsv in stdout
        assert expected_csv in stdout
        assert expected_txt in stdout
        assert expected_json in stdout

    def test_show_all_should_be_current_dir_agnostic(self):
        os.chdir(self.DATA_DIR)

        metrics = self.dvc.metrics.show(all_branches=True)
        self.assertMetricsHaveRelativePaths(metrics)

    def assertMetricsHaveRelativePaths(self, metrics):
        root_relpath = os.path.relpath(self.dvc.root_dir)
        metric_path = os.path.join(root_relpath, "metric")
        metric_json_path = os.path.join(root_relpath, "metric_json")
        metric_tsv_path = os.path.join(root_relpath, "metric_tsv")
        metric_htsv_path = os.path.join(root_relpath, "metric_htsv")
        metric_csv_path = os.path.join(root_relpath, "metric_csv")
        metric_hcsv_path = os.path.join(root_relpath, "metric_hcsv")
        metric_json_ext_path = os.path.join(root_relpath, "metric_json_ext")
        for branch in ["bar", "baz", "foo"]:
            self.assertEqual(
                set(metrics[branch].keys()),
                {
                    metric_path,
                    metric_json_path,
                    metric_tsv_path,
                    metric_htsv_path,
                    metric_csv_path,
                    metric_hcsv_path,
                    metric_json_ext_path,
                },
            )


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


class TestMetricsCLI(TestMetricsBase):
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

    def test_wrong_type_add(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["add", "metric.unknown"])
        assert ret == 0

        self._caplog.clear()
        ret = main(["metrics", "add", "metric.unknown", "-t", "unknown"])
        assert ret == 1

        assert (
            "failed to add metric file 'metric.unknown'"
        ) in self._caplog.text

        assert (
            "'unknown' is not supported, must be one of "
            "[raw, json, csv, tsv, hcsv, htsv]"
        ) in self._caplog.text

        ret = main(["metrics", "add", "metric.unknown", "-t", "raw"])
        assert ret == 0

        self._caplog.clear()
        ret = main(["metrics", "show", "metric.unknown"])
        assert ret == 0

        assert "\tmetric.unknown: unknown" in self._caplog.text

    def test_wrong_type_modify(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["run", "-m", "metric.unknown"])
        assert ret == 0

        self._caplog.clear()

        ret = main(["metrics", "modify", "metric.unknown", "-t", "unknown"])
        assert ret == 1

        assert "failed to modify metric file settings" in self._caplog.text

        assert (
            "metric type 'unknown' is not supported, must be one of "
            "[raw, json, csv, tsv, hcsv, htsv]"
        ) in self._caplog.text

        ret = main(["metrics", "modify", "metric.unknown", "-t", "CSV"])
        assert ret == 0

        self._caplog.clear()

        ret = main(["metrics", "show", "metric.unknown"])
        assert ret == 0

        assert "\tmetric.unknown: unknown" in self._caplog.text

    def test_wrong_type_show(self):
        with open("metric.unknown", "w+") as fd:
            fd.write("unknown")
            fd.flush()

        ret = main(["run", "-m", "metric.unknown"])
        assert ret == 0

        self._caplog.clear()

        ret = main(
            ["metrics", "show", "metric.unknown", "-t", "unknown", "-x", "0,0"]
        )
        assert ret == 0
        assert "\tmetric.unknown: unknown" in self._caplog.text


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
        assert not os.path.exists("metrics.json")

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


class TestMetricsType(TestDvc):
    branches = ["foo", "bar", "baz"]
    files = [
        "metric",
        "metric.txt",
        "metric.json",
        "metric.tsv",
        "metric.htsv",
        "metric.csv",
        "metric.hcsv",
    ]
    xpaths = [None, None, "branch", "0,0", "0,branch", "0,0", "0,branch"]

    def setUp(self):
        super(TestMetricsType, self).setUp()
        self.dvc.scm.commit("init")

        for branch in self.branches:
            self.dvc.scm.checkout(branch, create_new=True)
            with open("metric", "w+") as fd:
                fd.write(branch)
            with open("metric.txt", "w+") as fd:
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
            self.dvc.run(metrics_no_cache=self.files, overwrite=True)
            self.dvc.scm.add(self.files + ["metric.dvc"])
            self.dvc.scm.commit("metric")

        self.dvc.scm.checkout("master")

    def test_show(self):
        for file_name, xpath in zip(self.files, self.xpaths):
            self._do_show(file_name, xpath)

    def _do_show(self, file_name, xpath):
        ret = self.dvc.metrics.show(file_name, xpath=xpath, all_branches=True)
        self.assertEqual(len(ret), 3)
        for branch in self.branches:
            if isinstance(ret[branch][file_name], list):
                self.assertSequenceEqual(ret[branch][file_name], [branch])
            else:
                self.assertSequenceEqual(ret[branch][file_name], branch)


class TestShouldDisplayMetricsEvenIfMetricIsMissing(object):
    BRANCH_MISSING_METRIC = "missing_metric_branch"
    METRIC_FILE = "metric"
    METRIC_FILE_STAGE = METRIC_FILE + Stage.STAGE_FILE_SUFFIX

    def _write_metric(self):
        with open(self.METRIC_FILE, "w+") as fd:
            fd.write("0.5")
            fd.flush()

    def _commit_metric(self, dvc, branch):
        dvc.scm.add([self.METRIC_FILE_STAGE])
        dvc.scm.commit("{} commit".format(branch))

    def setUp(self, dvc):
        dvc.scm.branch(self.BRANCH_MISSING_METRIC)

        self._write_metric()

        ret = main(["run", "-m", self.METRIC_FILE])
        assert 0 == ret

        self._commit_metric(dvc, "master")

    def test(self, dvc, caplog):
        self.setUp(dvc)

        dvc.scm.checkout(self.BRANCH_MISSING_METRIC)

        self._write_metric()
        ret = main(["run", "-M", self.METRIC_FILE])
        assert 0 == ret

        self._commit_metric(dvc, self.BRANCH_MISSING_METRIC)
        os.remove(self.METRIC_FILE)

        ret = main(["metrics", "show", "-a"])

        assert (
            NO_METRICS_FILE_AT_REFERENCE_WARNING.format(
                self.METRIC_FILE, self.BRANCH_MISSING_METRIC
            )
            in caplog.text
        )
        assert 0 == ret
