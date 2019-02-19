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
    _server_lock = threading.Lock()

    def __init__(self, handler="etag"):
        self._server_lock.acquire()
        handler_class = ETagHandler if handler == "etag" else ContentMD5Handler
        self.bind_port_by_any_means(handler_class)

    def bind_port_by_any_means(self, handler_class):
        import time

        # shutdowning/closing socket/server does not unbind port in time,
        # locking the server also does not bring results, hence
        # this method
        for i in range(10000):
            try:
                self.httpd = HTTPServer(("localhost", 8000), handler_class)
            except Exception:
                time.sleep(0.01)
                continue
            break

    def __enter__(self):
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

    def __exit__(self, *args):
        self.httpd.socket.close()
        self.httpd.shutdown()
        self.httpd.server_close()
        self._server_lock.release()
