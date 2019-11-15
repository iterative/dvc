import hashlib
import os
import threading

# Moved in Python 2 -> 3, used only here.
# Not adding to dvc.utils.compat to not load http.server for non-test runs.
try:
    from http.server import HTTPServer
except ImportError:
    from BaseHTTPServer import HTTPServer

from RangeHTTPServer import RangeRequestHandler


class TestRequestHandler(RangeRequestHandler):
    checksum_header = None

    def end_headers(self):
        # RangeRequestHandler only sends Accept-Ranges header if Range header
        # is present, see https://github.com/danvk/RangeHTTPServer/issues/23
        if not self.headers.get("Range"):
            self.send_header("Accept-Ranges", "bytes")

        # Add a checksum header
        if self.checksum_header:
            file = self.translate_path(self.path)

            if not os.path.isdir(file) and os.path.exists(file):
                with open(file, "r") as fd:
                    encoded_text = fd.read().encode("utf8")
                    checksum = hashlib.md5(encoded_text).hexdigest()
                    self.send_header(self.checksum_header, checksum)

        RangeRequestHandler.end_headers(self)


class ETagHandler(TestRequestHandler):
    checksum_header = "ETag"


class ContentMD5Handler(TestRequestHandler):
    checksum_header = "Content-MD5"


class StaticFileServer:
    _lock = threading.Lock()

    def __init__(self, handler_class=ETagHandler):
        self._lock.acquire()
        self._httpd = HTTPServer(("localhost", 0), handler_class)
        self._thread = None

    def __enter__(self):
        self._thread = threading.Thread(target=self._httpd.serve_forever)
        self._thread.daemon = True
        self._thread.start()
        return self._httpd

    def __exit__(self, *args):
        self._httpd.socket.close()
        self._httpd.shutdown()
        self._httpd.server_close()
        self._lock.release()
