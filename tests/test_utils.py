import os
import shutil
import filecmp

from dvc import utils
from tests.basic_env import TestDvc


class TestUtils(TestDvc):
    def test_copyfile(self):
        src = "file1"
        dest = "file2"
        dest_dir = "testdir"

        with open(src, "w+") as f:
            f.write("file1contents")

        os.mkdir(dest_dir)

        utils.copyfile(src, dest)
        self.assertTrue(filecmp.cmp(src, dest, shallow=False))

        utils.copyfile(src, dest_dir)
        self.assertTrue(
            filecmp.cmp(src, "{}/{}".format(dest_dir, src), shallow=False)
        )

        shutil.rmtree(dest_dir)
        os.remove(src)
        os.remove(dest)

    def test_file_md5_crlf(self):
        with open("cr", "wb+") as fd:
            fd.write(b"a\nb\nc")
        with open("crlf", "wb+") as fd:
            fd.write(b"a\r\nb\r\nc")

        self.assertEqual(utils.file_md5("cr")[0], utils.file_md5("crlf")[0])

    def test_dict_md5(self):
        d = {
            "cmd": "python code.py foo file1",
            "locked": "true",
            "outs": [
                {
                    "path": "file1",
                    "metric": {"type": "raw"},
                    "cache": False,
                    "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
                }
            ],
            "deps": [
                {"path": "foo", "md5": "acbd18db4cc2f85cedef654fccc4a4d8"},
                {"path": "code.py", "md5": "d05447644b89960913c7eee5fd776adb"},
            ],
        }

        md5 = "8b263fa05ede6c3145c164829be694b4"

        self.assertEqual(md5, utils.dict_md5(d, exclude=["metric", "locked"]))

    def test_boxify(self):
        expected = (
            "+-----------------+\n"
            "|                 |\n"
            "|     message     |\n"
            "|                 |\n"
            "+-----------------+\n"
        )

        assert expected == utils.boxify("message")
