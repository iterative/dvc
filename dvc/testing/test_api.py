from dvc import api
from dvc.utils.fs import remove


class TestAPI:
    def test_get_url(self, tmp_dir, dvc, remote):
        tmp_dir.dvc_gen("foo", "foo")

        expected_url = (remote / "ac/bd18db4cc2f85cedef654fccc4a4d8").url
        assert api.get_url("foo") == expected_url

    def test_open(self, tmp_dir, dvc, remote):
        tmp_dir.dvc_gen("foo", "foo-text")
        dvc.push()

        # Remove cache to force download
        remove(dvc.odb.local.cache_dir)

        with api.open("foo") as fd:
            assert fd.read() == "foo-text"
