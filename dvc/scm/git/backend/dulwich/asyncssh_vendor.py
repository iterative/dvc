"""asyncssh SSH vendor for Dulwich."""
from typing import List, Optional

from dulwich.client import SSHVendor

from dvc.scm.asyn import BaseAsyncObject, sync_wrapper


class _StderrWrapper:
    def __init__(self, stderr):
        self.stderr = stderr

    async def _readlines(self):
        lines = []
        while True:
            line = await self.stderr.readline()
            if not line:
                break
            lines.append(line)
        return lines

    readlines = sync_wrapper(_readlines)


class AsyncSSHWrapper(BaseAsyncObject):
    def __init__(self, conn, proc, **kwargs):
        super().__init__(**kwargs)
        self.conn = conn
        self.proc = proc
        self.stderr = _StderrWrapper(proc.stderr)

    def can_read(self) -> bool:
        # pylint:disable=protected-access
        return self.proc.stdout._session._recv_buf_len > 0

    async def _read(self, n: Optional[int] = None) -> bytes:
        if self.proc.stdout.at_eof():
            return b""

        return await self.proc.stdout.read(n=n if n is not None else -1)

    read = sync_wrapper(_read)

    def write(self, data: bytes):
        self.proc.stdin.write(data)

    def close(self):
        self.conn.close()


class AsyncSSHVendor(BaseAsyncObject, SSHVendor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def _run_command(
        self,
        host: str,
        command: List[str],
        username: Optional[str] = None,
        port: Optional[int] = None,
        password: Optional[str] = None,
        key_filename: Optional[str] = None,
        **kwargs,
    ):
        """Connect to an SSH server.

        Run a command remotely and return a file-like object for interaction
        with the remote command.

        Args:
          host: Host name
          command: Command to run (as argv array)
          username: Optional ame of user to log in as
          port: Optional SSH port to use
          password: Optional ssh password for login or private key
          key_filename: Optional path to private keyfile
        """
        import asyncssh

        conn = await asyncssh.connect(
            host,
            port=port,
            username=username,
            password=password,
            client_keys=[key_filename] if key_filename else [],
            known_hosts=None,
            encoding=None,
        )
        proc = await conn.create_process(command, encoding=None)
        return AsyncSSHWrapper(conn, proc)

    run_command = sync_wrapper(_run_command)
