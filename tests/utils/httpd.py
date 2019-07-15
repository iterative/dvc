import hashlib
import os
import threading

from dvc.utils.compat import HTTPServer, SimpleHTTPRequestHandler


class ETagHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, "r") as fd:
                etag = hashlib.md5(fd.read().encode("utf8")).hexdigest()
                self.send_header("ETag", etag)

        SimpleHTTPRequestHandler.end_headers(self)


class ContentMD5Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, "r") as fd:
                md5 = hashlib.md5(fd.read().encode("utf8")).hexdigest()
                self.send_header("Content-MD5", md5)

        SimpleHTTPRequestHandler.end_headers(self)


class StaticFileServer:
    _lock = threading.Lock()

    def __init__(self, handler="etag"):
        self._lock.acquire()
        handler_class = ETagHandler if handler == "etag" else ContentMD5Handler
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
