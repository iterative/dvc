import io
from contextlib import contextmanager

from dvc.utils.compat import FileNotFoundError


@contextmanager
def open_url(url, mode="r", encoding=None):
    """Opens an url as a readable stream.

    Resumes on connection error.
    Url could be a string or a callable returning a string.
    """
    assert mode in {"r", "rt", "rb"}

    with iter_url(url) as (response, it):
        bytes_stream = IterStream(it)

        if mode == "rb":
            yield bytes_stream
        else:
            encoding = encoding or response.encoding
            yield io.TextIOWrapper(bytes_stream, encoding=encoding)


@contextmanager
def iter_url(url, chunk_size=io.DEFAULT_BUFFER_SIZE):
    """Iterate over chunks requested from url."""
    import requests

    def request(headers=None):
        the_url = url() if callable(url) else url
        response = requests.get(the_url, stream=True, headers=headers)
        if response.status_code == 404:
            raise FileNotFoundError("Can't open {}".format(the_url))
        response.raise_for_status()
        return response

    def gen(response):
        try:
            pos = 0
            while True:
                try:
                    for chunk in response.iter_content(chunk_size):
                        pos += len(chunk)
                        yield chunk
                    break
                except requests.ConnectionError:
                    response.close()
                    if response.headers.get("Accept-Ranges") != "bytes":
                        raise

                    # Reopen request from where we stopped
                    headers = {"Range": "bytes={}-".format(pos)}
                    response = request(headers)
        finally:
            response.close()

    response = request()
    it = gen(response)
    try:
        yield response, it
    finally:
        # Ensure connection is closed
        it.close()


class IterStream(io.RawIOBase):
    """Wraps an iterator yielding bytes as a file object"""

    def __init__(self, iterator):
        self.iterator = iterator
        self.leftover = None

    def readable(self):
        return True

    # Python 3 requires only .readinto() method, it still uses other ones
    # under some circumstances and falls back if those are absent. Since
    # iterator already constructs byte strings for us, .readinto() is not the
    # most optimal, so we provide .read1() too.

    def readinto(self, b):
        try:
            n = len(b)  # We're supposed to return at most this much
            chunk = self.leftover or next(self.iterator)
            output, self.leftover = chunk[:n], chunk[n:]

            n_out = len(output)
            b[:n_out] = output
            return n_out
        except StopIteration:
            return 0  # indicate EOF

    readinto1 = readinto

    def read1(self, n=-1):
        try:
            chunk = self.leftover or next(self.iterator)
        except StopIteration:
            return b""

        # Return an arbitrary number or bytes
        if n <= 0:
            self.leftover = None
            return chunk

        output, self.leftover = chunk[:n], chunk[n:]
        return output
