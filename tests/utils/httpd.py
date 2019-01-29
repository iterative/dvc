import hashlib
import os
import threading

from dvc.utils.compat import HTTPServer, SimpleHTTPRequestHandler


class ETagHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, 'r') as fd:
                etag = hashlib.md5(fd.read().encode('utf8')).hexdigest()
                self.send_header('ETag', etag)

        SimpleHTTPRequestHandler.end_headers(self)


class ContentMD5Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, 'r') as fd:
                md5 = hashlib.md5(fd.read().encode('utf8')).hexdigest()
                self.send_header('Content-MD5', md5)

        SimpleHTTPRequestHandler.end_headers(self)


class StaticFileServer:
    __server_lock = threading.Lock()

    def __init__(self, handler='etag'):
        handler_class = ETagHandler if handler == 'etag' else ContentMD5Handler

        self.__server_lock.acquire()
        self.httpd = HTTPServer(('localhost', 8000), handler_class)

    def __enter__(self):
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

    def __exit__(self, *args):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.__server_lock.release()
