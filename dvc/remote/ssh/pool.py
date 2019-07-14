from collections import deque
from contextlib import contextmanager
from funcy import memoize

from .connection import SSHConnection


@contextmanager
def ssh_connection(*conn_args, **conn_kwargs):
    pool = get_ssh_pool(*conn_args, **conn_kwargs)
    conn = pool.get_connection()
    try:
        yield conn
    except BaseException:
        conn.close()
    else:
        pool.release(conn)


@memoize
def get_ssh_pool(*conn_args, **conn_kwargs):
    return SSHPool(conn_args, conn_kwargs)


class SSHPool(object):
    def __init__(self, conn_args, conn_kwargs):
        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs
        self._conns = deque()
        self._closed = False

    def __del__(self):
        self.close()

    def close(self):
        while self._conns:
            self._conns.pop().close()
        self._closed = True

    def get_connection(self):
        try:
            return self._conns.popleft()
        except IndexError:
            return SSHConnection(*self._conn_args, **self._conn_kwargs)

    def release(self, conn):
        if self._closed:
            conn.close()
        else:
            self._conns.append(conn)
