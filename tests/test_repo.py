from tests.basic_env import TestDvc

from dvc.scm.git import GitTree
from dvc.scm.tree import WorkingTree


class TestCollect(TestDvc):
    def setUp(self):
        super(TestCollect, self).setUp()
        self.dvc.add(self.FOO)
        self.dvc.run(
            deps=[self.FOO],
            outs=[self.BAR],
            cmd="python code.py {} {}".format(self.FOO, self.BAR),
        )
        self.dvc.scm.add([".gitignore", self.FOO + ".dvc", self.BAR + ".dvc"])
        self.dvc.scm.commit("foo.dvc and bar.dvc")
        self.dvc.scm.checkout("new_branch", True)
        self.dvc.run(
            deps=[self.BAR],
            outs=["buzz"],
            cmd="python code.py {} {}".format(self.BAR, "buzz"),
        )
        self.dvc.scm.add([".gitignore", "buzz.dvc"])
        self.dvc.scm.commit("add buzz")
        self.dvc.scm.checkout("master")

    def _check(self, branch, target, with_deps, expected):
        if branch:
            self.dvc.tree = GitTree(self.dvc.scm.git, branch)
        else:
            self.dvc.tree = WorkingTree()
        result = self.dvc.collect(target + ".dvc", with_deps=with_deps)
        self.assertEqual(
            [[j.rel_path for j in i.outs] for i in result], expected
        )
        return result

    def test(self):
        self._check("", self.BAR, True, [[self.FOO], [self.BAR]])
        self._check("master", self.BAR, True, [[self.FOO], [self.BAR]])
        self._check(
            "new_branch", "buzz", True, [[self.FOO], [self.BAR], ["buzz"]]
        )
        result = self._check("new_branch", "buzz", False, [["buzz"]])
        self.assertEqual([i.rel_path for i in result[0].deps], ["bar"])
