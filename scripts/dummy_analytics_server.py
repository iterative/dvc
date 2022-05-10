import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from json import JSONDecodeError

from rich.console import Console

from dvc.env import DVC_ANALYTICS_HOST

console = Console(stderr=True)


class RequestHandler(BaseHTTPRequestHandler):
    def _respond(self, body: str = "", status_code: int = 200):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        content = {"body": body, "statusCode": status_code}
        self.wfile.write(json.dumps(content).encode("utf-8"))

    do_GET = _respond

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length)
        data = post_data.decode("utf-8")
        self._respond(data)

        for key, value in self.headers.items():
            console.print(f"[blue]{key}[/blue]: ", end="")
            console.print(value, highlight=False, markup=False)

        try:
            console.print_json(data)
        except JSONDecodeError:
            console.print(data)
        console.print()


def run(
    server_class=ThreadingHTTPServer, handler_class=RequestHandler, port=8000
):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    host, port = httpd.socket.getsockname()[:2]
    url_host = f"[{host}]" if ":" in host else host
    console.print(
        f"Serving on {host} port {port} (http://{url_host}:{port}/) ..."
    )
    console.print(
        f'Set {DVC_ANALYTICS_HOST}="{url_host}:{port}" '
        "to test with dummy server.\n"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, exiting.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "port",
        action="store",
        default=8000,
        type=int,
        nargs="?",
        help="Specify alternate port [default: %(default)s].",
    )
    args = parser.parse_args()
    run(port=args.port)
