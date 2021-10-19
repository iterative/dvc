import hashlib
import os
import threading
from contextlib import contextmanager
from http import HTTPStatus
from http.server import HTTPServer

from RangeHTTPServer import RangeRequestHandler


class TestRequestHandler(RangeRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def end_headers(self):
        # RangeRequestHandler only sends Accept-Ranges header if Range header
        # is present, see https://github.com/danvk/RangeHTTPServer/issues/23
        if not self.headers.get("Range"):
            self.send_header("Accept-Ranges", "bytes")

        # Add a checksum header
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, encoding="utf-8") as fd:
                encoded_text = fd.read().encode("utf8")
                checksum = hashlib.md5(encoded_text).hexdigest()
                self.send_header("Content-MD5", checksum)

        RangeRequestHandler.end_headers(self)

    def _chunks(self):
        while True:
            data = self.rfile.readline(65537)
            chunk_size = int(data[:-2], 16)
            if chunk_size == 0:
                return
            data = self.rfile.read(chunk_size)
            yield data
            self.rfile.read(2)

    def do_POST(self):
        chunked = self.headers.get("Transfer-Encoding", "") == "chunked"
        path = self.translate_path(self.path)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fd:
                if chunked:
                    for chunk in self._chunks():
                        fd.write(chunk)
                else:
                    size = int(self.headers.get("Content-Length", 0))
                    fd.write(self.rfile.read(size))
        except OSError as e:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))
        self.send_response(HTTPStatus.OK)
        self.end_headers()


@contextmanager
def run_server_on_thread(server):
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    yield server

    server.socket.close()
    server.shutdown()
    server.server_close()


class StaticFileServer:
    _lock = threading.Lock()

    def __init__(self, directory):
        from functools import partial

        addr = ("localhost", 0)
        req = partial(TestRequestHandler, directory=directory)
        server = HTTPServer(addr, req)
        self.runner = run_server_on_thread(server)

    # pylint: disable=no-member
    def __enter__(self):
        self._lock.acquire()
        return self.runner.__enter__()

    def __exit__(self, *args):
        self.runner.__exit__(*args)
        self._lock.release()
