from dvc.scm.git.pre_commit_tool import pre_commit_tool_conf
from dvc.scm.git.pre_commit_tool import merge_pre_commit_tool_confs

from unittest import TestCase


class TestPreCommitTool(TestCase):
    def setUp(self):
        self.conf = pre_commit_tool_conf("a", "b", "c")

    def test_merge_pre_commit_tool_confs_empty(self):
        existing_conf = None
        merged_conf = merge_pre_commit_tool_confs(existing_conf, self.conf)
        self.assertEqual(self.conf, merged_conf)

    def test_merge_pre_commit_tool_confs_invalid_yaml(self):
        existing_conf = "some invalid yaml"
        merged_conf = merge_pre_commit_tool_confs(existing_conf, self.conf)
        self.assertEqual(self.conf, merged_conf)

    def test_merge_pre_commit_tool_confs_no_repos(self):
        existing_conf = {"foo": [1, 2, 3]}
        merged_conf = merge_pre_commit_tool_confs(existing_conf, self.conf)
        self.assertEqual(self.conf, merged_conf)

    def test_merge_pre_commit_tool_confs(self):
        existing_conf = {"repos": [{}]}
        merged_conf = merge_pre_commit_tool_confs(existing_conf, self.conf)
        # Merging the new conf in should append the new repo to the end of
        # the existing repos array on the existing conf.
        self.assertEqual(self.conf["repos"][0], merged_conf["repos"][1])
