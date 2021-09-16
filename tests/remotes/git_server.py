import logging
import os
import subprocess

import pytest
from mockssh.streaming import Stream, StreamTransfer

from .ssh import TEST_SSH_USER, SSHMocked, SSHMockHandler


class InvalidGitCommandError(Exception):
    pass


class GitPackTransfer(StreamTransfer):
    """Git pack-protocol transfer for server-side git commands."""

    log = logging.getLogger(__name__)

    FLUSH_PKT = b"0000"
    PACK_HEADER = b"PACK"

    def __init__(self, ssh_channel, process):
        super().__init__(ssh_channel, process)
        self.pkt_mode = True

    def ssh_to_process(self, channel, process_stream):
        return Stream(
            channel,
            lambda: channel.recv(self.BUFFER_SIZE),
            process_stream.write,
            process_stream.flush,
        )

    def process_to_ssh(self, process_stream, write_func):
        return Stream(
            process_stream,
            lambda: self._read_pkt_line(process_stream),
            write_func,
            lambda: None,
        )

    def _read_pkt_line(self, process_stream):
        if not self.pkt_mode:
            return process_stream.readline()
        sizestr = process_stream.read(4)
        if sizestr == self.FLUSH_PKT:
            return sizestr
        if sizestr == self.PACK_HEADER:
            self.pkt_mode = False
            return process_stream.readline()
        return sizestr + process_stream.read(int(sizestr, 16))


class GitSSHHandler(SSHMockHandler):
    """Handler for Git server operations."""

    log = logging.getLogger(__name__)

    # Git server only allows a limited subset of commands to be run over SSH
    GIT_SHELL_COMMANDS = {
        b"git-receive-pack",
        b"git-upload-pack",
        b"git-upload-archive",
        b"receive-pack",
        b"upload-pack",
        b"upload-archive",
    }

    def handle_client(self, channel):
        command = self.command_queues[channel.chanid].get(block=True)
        self.log.debug("Executing %s", command)
        try:
            self._check_command(command)
            transfer_cls = GitPackTransfer
            self.log.debug("Using git-pack transfer")
        except InvalidGitCommandError:
            transfer_cls = StreamTransfer
            self.log.debug("Using stream transfer")
        try:
            p = subprocess.Popen(
                command,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            transfer_cls(channel, p).run()
            channel.send_exit_status(p.returncode)
        except Exception:  # pylint: disable=broad-except
            self.log.error(
                "Error handling client (channel: %s)",
                channel,
                exc_info=True,
            )
        finally:
            try:
                channel.close()
            except EOFError:
                self.log.debug("Tried to close already closed channel")

    @classmethod
    def _check_command(cls, command):
        args = command.split()
        cmd = os.path.basename(args[0])
        if cmd in (b"git", b"git.exe"):
            cmd = os.path.basename(args[1])
        if cmd not in cls.GIT_SHELL_COMMANDS:
            raise InvalidGitCommandError


@pytest.fixture
def git_server(ssh_server):
    ssh_server.handler_cls = GitSSHHandler
    yield ssh_server


@pytest.fixture
def git_ssh(git_server, monkeypatch):
    from dvc.fs.ssh import SSHFileSystem

    # NOTE: see http://github.com/iterative/dvc/pull/3501
    monkeypatch.setattr(SSHFileSystem, "CAN_TRAVERSE", False)

    return SSHMocked(SSHMocked.get_url(TEST_SSH_USER, git_server.port))
