import os
import posixpath
import logging
import errno
import stat
from funcy import cached_property

try:
    import paramiko
except ImportError:
    paramiko = None

from dvc.utils import tmp_fname
from dvc.utils.compat import ignore_file_not_found
from dvc.progress import Tqdm
from dvc.exceptions import DvcException
from dvc.remote.base import RemoteCmdError


logger = logging.getLogger(__name__)


def sizeof_fmt(num, suffix="B"):
    """ Convert number of bytes to human-readable string """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Y", suffix)


class SSHConnection:
    def __init__(self, host, *args, **kwargs):
        logger.debug(
            "Establishing ssh connection with '{host}' "
            "through port '{port}' as user '{username}'".format(
                host=host, **kwargs
            )
        )
        self.timeout = kwargs.get("timeout", 1800)

        self._ssh = paramiko.SSHClient()
        self._ssh.load_system_host_keys()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        self._ssh.connect(host, *args, **kwargs)
        self._ssh.get_transport().set_keepalive(10)
        self._sftp_channels = []

    @property
    def sftp(self):
        if not self._sftp_channels:
            self._sftp_channels = [self._ssh.open_sftp()]
        return self._sftp_channels[0]

    def close(self):
        for sftp in self._sftp_channels:
            sftp.close()
        self._ssh.close()

    def st_mode(self, path):
        with ignore_file_not_found():
            return self.sftp.stat(path).st_mode

        return 0

    def exists(self, path, sftp=None):
        return bool(self.st_mode(path))

    def isdir(self, path):
        return stat.S_ISDIR(self.st_mode(path))

    def isfile(self, path):
        return stat.S_ISREG(self.st_mode(path))

    def islink(self, path):
        return stat.S_ISLNK(self.st_mode(path))

    def makedirs(self, path):
        # Single stat call will say whether this is a dir, a file or a link
        st_mode = self.st_mode(path)

        if stat.S_ISDIR(st_mode):
            return

        if stat.S_ISREG(st_mode) or stat.S_ISLNK(st_mode):
            raise DvcException(
                "a file with the same name '{}' already exists".format(path)
            )

        head, tail = posixpath.split(path)

        if head:
            self.makedirs(head)

        if tail:
            try:
                self.sftp.mkdir(path)
            except IOError as e:
                # Since paramiko errors are very vague we need to recheck
                # whether it's because path already exists or something else
                if e.errno == errno.EACCES or not self.exists(path):
                    raise

    def walk(self, directory, topdown=True):
        # NOTE: original os.walk() implementation [1] with default options was
        # used as a template.
        #
        # [1] https://github.com/python/cpython/blob/master/Lib/os.py
        try:
            dir_entries = self.sftp.listdir_attr(directory)
        except IOError as exc:
            raise DvcException(
                "couldn't get the '{}' remote directory files list".format(
                    directory
                ),
                cause=exc,
            )

        dirs = []
        nondirs = []
        for entry in dir_entries:
            name = entry.filename
            if stat.S_ISDIR(entry.st_mode):
                dirs.append(name)
            else:
                nondirs.append(name)

        if topdown:
            yield directory, dirs, nondirs

        for dname in dirs:
            newpath = posixpath.join(directory, dname)
            for entry in self.walk(newpath, topdown=topdown):
                yield entry

        if not topdown:
            yield directory, dirs, nondirs

    def walk_files(self, directory):
        for root, dirs, files in self.walk(directory):
            for fname in files:
                yield posixpath.join(root, fname)

    def _remove_file(self, path):
        with ignore_file_not_found():
            self.sftp.remove(path)

    def _remove_dir(self, path):
        for root, dirs, files in self.walk(path, topdown=False):
            for fname in files:
                path = posixpath.join(root, fname)
                with ignore_file_not_found():
                    self._remove_file(path)

            for dname in dirs:
                path = posixpath.join(root, dname)
                with ignore_file_not_found():
                    self.sftp.rmdir(dname)

        with ignore_file_not_found():
            self.sftp.rmdir(path)

    def remove(self, path):
        if self.isdir(path):
            self._remove_dir(path)
        else:
            self._remove_file(path)

    def download(self, src, dest, no_progress_bar=False, progress_title=None):
        if not progress_title:
            progress_title = os.path.basename(src)

        with Tqdm(
            desc_truncate=progress_title,
            disable=no_progress_bar
        ) as pbar:
            self.sftp.get(src, dest, callback=pbar.update_to)

    def move(self, src, dst):
        self.makedirs(posixpath.dirname(dst))
        self.sftp.rename(src, dst)

    def upload(self, src, dest, no_progress_bar=False, progress_title=None):
        self.makedirs(posixpath.dirname(dest))
        tmp_file = tmp_fname(dest)
        if not progress_title:
            progress_title = posixpath.basename(dest)

        with Tqdm(
            desc_truncate=progress_title,
            disable=no_progress_bar
        ) as pbar:
            self.sftp.put(src, tmp_file, callback=pbar.update_to)

        self.sftp.rename(tmp_file, dest)

    def execute(self, cmd):
        stdin, stdout, stderr = self._ssh.exec_command(cmd)
        channel = stdout.channel

        stdin.close()
        channel.shutdown_write()

        stdout_chunks = []
        stderr_chunks = []
        while (
            not channel.closed
            or channel.recv_ready()
            or channel.recv_stderr_ready()
        ):
            import select

            got_chunk = False
            readq, _, _ = select.select([stdout.channel], [], [], self.timeout)
            for c in readq:
                if c.recv_ready():
                    stdout_chunks.append(stdout.channel.recv(len(c.in_buffer)))
                    got_chunk = True

                if c.recv_stderr_ready():
                    stderr_len = len(c.in_stderr_buffer)
                    s = stderr.channel.recv_stderr(stderr_len)
                    stderr_chunks.append(s)
                    got_chunk = True

            if (
                not got_chunk
                and stdout.channel.exit_status_ready()
                and not stderr.channel.recv_stderr_ready()
                and not stdout.channel.recv_ready()
            ):
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()

        ret = stdout.channel.recv_exit_status()
        if ret != 0:
            err = b"".join(stderr_chunks).decode("utf-8")
            raise RemoteCmdError("ssh", cmd, ret, err)

        return b"".join(stdout_chunks).decode("utf-8")

    @cached_property
    def uname(self):
        return self.execute("uname").strip()

    def md5(self, path):
        """
        Use different md5 commands depending on the OS:

         - Darwin's `md5` returns BSD-style checksums by default
         - Linux's `md5sum` needs the `--tag` flag for a similar output

         Example:
              MD5 (foo.txt) = f3d220a856b52aabbf294351e8a24300
        """
        if self.uname == "Linux":
            md5 = self.execute("md5sum " + path).split()[0]
        elif self.uname == "Darwin":
            md5 = self.execute("md5 " + path).split()[-1]
        else:
            raise DvcException(
                "'{}' is not supported as a SSH remote".format(self.uname)
            )

        assert len(md5) == 32
        return md5

    def cp(self, src, dest):
        self.makedirs(posixpath.dirname(dest))
        self.execute("cp {} {}".format(src, dest))

    def open_max_sftp_channels(self):
        # If there are more than 1 it means we've already opened max amount
        if len(self._sftp_channels) <= 1:
            while True:
                try:
                    self._sftp_channels.append(self._ssh.open_sftp())
                except paramiko.ssh_exception.ChannelException:
                    if not self._sftp_channels:
                        raise
                    break
        return self._sftp_channels
