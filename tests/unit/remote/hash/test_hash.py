from dvc.main import main
from dvc.utils.stage import load_stage_file
from tests.basic_env import TestDvc


class TestHash(TestDvc):
    def test_compatibility(self):
        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        ret = main(["config", "hash.local", "sha256,md5"])
        self.assertEqual(0, ret)

        ret = main(["status"])
        self.assertEqual(0, ret)

        ret = main(["status", "-q"])
        self.assertEqual(0, ret)

        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        d = load_stage_file("foo.dvc")
        self.assertEqual(
            d["sha256"],
            "6bc1683385ab4218fed1f53195c41d95d186c085a9e929cd77f012d4f541dd34",
        )

    def test_incompatibility(self):
        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        ret = main(["config", "hash.local", "sha256"])
        self.assertEqual(0, ret)

        ret = main(["status"])
        self.assertEqual(0, ret)

        ret = main(["status", "-q"])
        self.assertEqual(1, ret)

        ret = main(["add", self.FOO])
        self.assertEqual(0, ret)

        d = load_stage_file("foo.dvc")
        self.assertEqual(
            d["sha256"],
            "6bc1683385ab4218fed1f53195c41d95d186c085a9e929cd77f012d4f541dd34",
        )
