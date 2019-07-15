import dvc

from dvc.utils.compat import str

import os
from uuid import uuid4

from dvc.utils.compat import urljoin
from dvc.main import main
from mock import patch, mock_open, call
from tests.basic_env import TestDvc
from tests.utils import spy
from tests.utils.httpd import StaticFileServer


class TestCmdImport(TestDvc):
    def test(self):
        ret = main(["import-url", self.FOO, "import"])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists("import.dvc"))

        ret = main(["import-url", "non-existing-file", "import"])
        self.assertNotEqual(ret, 0)

    def test_unsupported(self):
        ret = main(["import-url", "unsupported://path", "import_unsupported"])
        self.assertNotEqual(ret, 0)


class TestDefaultOutput(TestDvc):
    def test(self):
        tmpdir = self.mkdtemp()
        filename = str(uuid4())
        tmpfile = os.path.join(tmpdir, filename)

        with open(tmpfile, "w") as fd:
            fd.write("content")

        ret = main(["import-url", tmpfile])
        self.assertEqual(ret, 0)
        self.assertTrue(os.path.exists(filename))
        with open(filename) as fd:
            self.assertEqual(fd.read(), "content")


class TestInterruptedDownload(TestDvc):
    def _prepare_interrupted_download(self, port):
        import_url = urljoin("http://localhost:{}/".format(port), self.FOO)
        import_output = "imported_file"
        tmp_file_name = import_output + ".part"
        tmp_file_path = os.path.realpath(
            os.path.join(self._root_dir, tmp_file_name)
        )
        self._import_with_interrupt(import_output, import_url)
        self.assertTrue(os.path.exists(tmp_file_name))
        self.assertFalse(os.path.exists(import_output))
        return import_output, import_url, tmp_file_path

    def _import_with_interrupt(self, import_output, import_url):
        def interrupting_generator():
            yield self.FOO[0].encode("utf8")
            raise KeyboardInterrupt

        with patch(
            "requests.models.Response.iter_content",
            return_value=interrupting_generator(),
        ):
            with patch(
                "dvc.remote.http.RemoteHTTP._content_length", return_value=3
            ):
                result = main(["import-url", import_url, import_output])
                self.assertEqual(result, 252)


class TestShouldResumeDownload(TestInterruptedDownload):
    @patch("dvc.remote.http.RemoteHTTP.CHUNK_SIZE", 1)
    def test(self):
        with StaticFileServer() as httpd:
            output, url, file_path = self._prepare_interrupted_download(
                httpd.server_port
            )

            m = mock_open()
            with patch("dvc.remote.http.open", m):
                result = main(["import-url", "--resume", url, output])
                self.assertEqual(result, 0)
        m.assert_called_once_with(file_path, "ab")
        m_handle = m()
        expected_calls = [call(b"o"), call(b"o")]
        m_handle.write.assert_has_calls(expected_calls, any_order=False)


class TestShouldNotResumeDownload(TestInterruptedDownload):
    @patch("dvc.remote.http.RemoteHTTP.CHUNK_SIZE", 1)
    def test(self):
        with StaticFileServer() as httpd:
            output, url, file_path = self._prepare_interrupted_download(
                httpd.server_port
            )

            m = mock_open()
            with patch("dvc.remote.http.open", m):
                result = main(["import-url", url, output])
                self.assertEqual(result, 0)
        m.assert_called_once_with(file_path, "wb")
        m_handle = m()
        expected_calls = [call(b"f"), call(b"o"), call(b"o")]
        m_handle.write.assert_has_calls(expected_calls, any_order=False)


class TestShouldRemoveOutsBeforeImport(TestDvc):
    def setUp(self):
        super(TestShouldRemoveOutsBeforeImport, self).setUp()
        tmp_dir = self.mkdtemp()
        self.external_source = os.path.join(tmp_dir, "file")
        with open(self.external_source, "w") as fobj:
            fobj.write("content")

    def test(self):
        remove_outs_call_counter = spy(dvc.stage.Stage.remove_outs)
        with patch.object(
            dvc.stage.Stage, "remove_outs", remove_outs_call_counter
        ):
            ret = main(["import-url", self.external_source])
            self.assertEqual(0, ret)

        self.assertEqual(1, remove_outs_call_counter.mock.call_count)


class TestImportFilename(TestDvc):
    def setUp(self):
        super(TestImportFilename, self).setUp()
        tmp_dir = self.mkdtemp()
        self.external_source = os.path.join(tmp_dir, "file")
        with open(self.external_source, "w") as fobj:
            fobj.write("content")

    def test(self):
        ret = main(["import-url", "-f", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")

        ret = main(["import-url", "--file", "bar.dvc", self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists("bar.dvc"))

        os.remove("bar.dvc")
        os.mkdir("sub")

        path = os.path.join("sub", "bar.dvc")
        ret = main(["import-url", "--file", path, self.external_source])
        self.assertEqual(0, ret)
        self.assertTrue(os.path.exists(path))
