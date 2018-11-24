import hashlib
import os
import threading

try:
    from http.server import HTTPServer, SimpleHTTPRequestHandler
except ImportError:
    from BaseHTTPServer import HTTPServer
    from SimpleHTTPServer import SimpleHTTPRequestHandler


class ETagHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        file = self.translate_path(self.path)

        if not os.path.isdir(file) and os.path.exists(file):
            with open(file, 'r') as fd:
                etag = hashlib.md5(fd.read().encode('utf8')).hexdigest()
                self.send_header('ETag', etag)

        SimpleHTTPRequestHandler.end_headers(self)


class StaticFileServer():
    def __init__(self):
        self.httpd = HTTPServer(('localhost', 8000), ETagHandler)

    def __enter__(self):
        self.server_thread = threading.Thread(target=self.httpd.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

    def __exit__(self, *args):
        self.httpd.shutdown()
        self.httpd.server_close()
