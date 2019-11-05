from __future__ import unicode_literals

import io

import requests

from dvc.utils.http import open_url
from tests.utils.httpd import StaticFileServer


def test_open_url(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    # Simulate bad connection
    original_iter_content = requests.Response.iter_content

    def bad_iter_content(self, *args, **kwargs):
        it = original_iter_content(self, *args, **kwargs)
        for i, chunk in enumerate(it):
            # Drop connection error on second chunk if there is one
            if i > 0:
                raise requests.ConnectionError("Simulated connection drop")
            yield chunk

    monkeypatch.setattr(requests.Response, "iter_content", bad_iter_content)

    # Text should be longer than default chunk to test resume,
    # using twice of that plus something tests second resume,
    # this is important because second response is different
    text = "0123456789" * (io.DEFAULT_BUFFER_SIZE // 10 + 1)
    (tmp_path / "sample.txt").write_text(text * 2)

    with StaticFileServer() as httpd:
        url = "http://localhost:{}/sample.txt".format(httpd.server_port)
        with open_url(url) as fd:
            # Test various .read() variants
            assert fd.read(len(text)) == text
            assert fd.read() == text
