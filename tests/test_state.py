import os

from dvc.utils.compat import str

from dvc.state import State
from dvc.utils import file_md5
from dvc.main import main

from tests.basic_env import TestDvc


class TestState(TestDvc):
    def test_update(self):
        path = os.path.join(self.dvc.root_dir, self.FOO)
        md5 = file_md5(path)[0]

        state = State(self.dvc, self.dvc.config.config)

        with state:
            entry_md5 = state.update(path)
            self.assertEqual(entry_md5, md5)

            os.unlink(path)
            with open(path, "a") as fd:
                fd.write("1")

            md5 = file_md5(path)[0]
            entry_md5 = state.update(path)
            self.assertEqual(entry_md5, md5)


class TestStateOverflow(TestDvc):
    def test(self):
        # NOTE: trying to add more entries than state can handle,
        # to see if it will clean up and vacuum successfully
        ret = main(["config", "state.row_limit", "10"])
        self.assertEqual(ret, 0)

        dname = "dir"
        os.mkdir(dname)
        for i in range(20):
            with open(os.path.join(dname, str(i)), "w+") as fobj:
                fobj.write(str(i))

        ret = main(["add", "dir"])
        self.assertEqual(ret, 0)
