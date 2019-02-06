from __future__ import unicode_literals

import os
import getpass
import posixpath

try:
    import paramiko
except ImportError:
    paramiko = None

import dvc.prompt as prompt
import dvc.logger as logger

from dvc.config import Config
from dvc.progress import progress
from dvc.utils.compat import urlparse
from dvc.exceptions import DvcException
from dvc.remote.base import RemoteBase, RemoteCmdError


def sizeof_fmt(num, suffix="B"):
    """ Convert number of bytes to human-readable string """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Y", suffix)


def percent_cb(name, complete, total):
    """ Callback for updating target progress """
    logger.debug(
        "{}: {} transferred out of {}".format(
            name, sizeof_fmt(complete), sizeof_fmt(total)
        )
    )
    progress.update_target(name, complete, total)


def create_cb(name):
    """ Create callback function for multipart object """
    return lambda cur, tot: percent_cb(name, cur, tot)


class RemoteSSH(RemoteBase):
    scheme = "ssh"

    # NOTE: we support both URL-like (ssh://[user@]host.xz[:port]/path) and
    # SCP-like (ssh://[user@]host.xz:path) urls.
    REGEX = r"^ssh://.*$"

    REQUIRES = {"paramiko": paramiko}

    JOBS = 4

    PARAM_CHECKSUM = "md5"

    DEFAULT_PORT = 22
    TIMEOUT = 1800

    def __init__(self, repo, config):
        super(RemoteSSH, self).__init__(repo, config)
        self.url = config.get(Config.SECTION_REMOTE_URL, "ssh://")

        parsed = urlparse(self.url)
        self.host = parsed.hostname
        self.user = (
            config.get(Config.SECTION_REMOTE_USER)
            or parsed.username
            or getpass.getuser()
        )
        self.prefix = parsed.path or "/"
        self.port = (
            config.get(Config.SECTION_REMOTE_PORT)
            or parsed.port
            or self.DEFAULT_PORT
        )
        self.keyfile = config.get(Config.SECTION_REMOTE_KEY_FILE, None)
        self.timeout = config.get(Config.SECTION_REMOTE_TIMEOUT, self.TIMEOUT)
        self.password = config.get(Config.SECTION_REMOTE_PASSWORD, None)
        self.ask_password = config.get(
            Config.SECTION_REMOTE_ASK_PASSWORD, False
        )

        self.path_info = {
            "scheme": "ssh",
            "host": self.host,
            "user": self.user,
            "port": self.port,
        }

    def ssh(self, host=None, user=None, port=None):
        msg = (
            "Establishing ssh connection with '{}' "
            "through port '{}' as user '{}'"
        )
        logger.debug(msg.format(host, port, user))

        ssh = paramiko.SSHClient()

        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.ask_password and self.password is None:
            msg = (
                "Enter a private key passphrase or a password for "
                "host '{}' port '{}' user '{}'"
            ).format(host, port, user)
            self.password = prompt.password(msg)

        ssh.connect(
            host,
            username=user,
            port=port,
            key_filename=self.keyfile,
            timeout=self.timeout,
            password=self.password,
        )

        return ssh

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info["scheme"] == "ssh"

        with self.ssh(
            path_info["host"], path_info["user"], path_info["port"]
        ) as ssh:
            try:
                self._exec(ssh, "test -e {}".format(path_info["path"]))
                exists = True
            except RemoteCmdError:
                exists = False

        return exists

    def _exec(self, ssh, cmd):
        stdin, stdout, stderr = ssh.exec_command(cmd)
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
            raise RemoteCmdError(self.scheme, cmd, ret, err)

        return b"".join(stdout_chunks).decode("utf-8")

    def md5(self, path_info):
        if path_info["scheme"] != "ssh":
            raise NotImplementedError

        ssh = self.ssh(
            host=path_info["host"],
            user=path_info["user"],
            port=path_info["port"],
        )

        # Use different md5 commands depending on os
        stdout = self._exec(ssh, "uname").strip()
        if stdout == "Darwin":
            md5cmd = "md5"
            index = -1
        elif stdout == "Linux":
            md5cmd = "md5sum"
            index = 0
        else:
            msg = "'{}' is not supported as a remote".format(stdout)
            raise DvcException(msg)

        stdout = self._exec(ssh, "{} {}".format(md5cmd, path_info["path"]))
        md5 = stdout.split()[index]
        ssh.close()

        assert len(md5) == 32

        return md5

    def copy(self, from_info, to_info, ssh=None):
        if from_info["scheme"] != "ssh" or to_info["scheme"] != "ssh":
            raise NotImplementedError

        assert from_info["host"] == to_info["host"]
        assert from_info["user"] == to_info["user"]

        s = (
            ssh
            if ssh
            else self.ssh(
                host=from_info["host"],
                user=from_info["user"],
                port=from_info["port"],
            )
        )

        dname = posixpath.dirname(to_info["path"])
        self._exec(s, "mkdir -p {}".format(dname))
        self._exec(s, "cp {} {}".format(from_info["path"], to_info["path"]))

        if not ssh:
            s.close()

    def save_info(self, path_info):
        if path_info["scheme"] != "ssh":
            raise NotImplementedError

        return {self.PARAM_CHECKSUM: self.md5(path_info)}

    def save(self, path_info, checksum_info):
        if path_info["scheme"] != "ssh":
            raise NotImplementedError

        md5 = checksum_info[self.PARAM_CHECKSUM]
        dest = path_info.copy()
        dest["path"] = self.checksum_to_path(md5)

        self.copy(path_info, dest)

    def remove(self, path_info):
        if path_info["scheme"] != "ssh":
            raise NotImplementedError

        logger.debug(
            "Removing ssh://{}@{}/{}".format(
                path_info["user"], path_info["host"], path_info["path"]
            )
        )

        ssh = self.ssh(
            host=path_info["host"],
            user=path_info["user"],
            port=path_info["port"],
        )
        ssh.open_sftp().remove(path_info["path"])
        ssh.close()

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)

        ssh = self.ssh(
            host=from_infos[0]["host"],
            user=from_infos[0]["user"],
            port=from_infos[0]["port"],
        )

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info["scheme"] != "ssh":
                raise NotImplementedError

            if to_info["scheme"] == "ssh":
                assert from_info["host"] == to_info["host"]
                assert from_info["port"] == to_info["port"]
                assert from_info["user"] == to_info["user"]
                self.copy(from_info, to_info, ssh=ssh)
                continue

            if to_info["scheme"] != "local":
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(
                from_info["host"], from_info["path"], to_info["path"]
            )
            logger.debug(msg)

            if not name:
                name = os.path.basename(to_info["path"])

            self._makedirs(to_info["path"])
            tmp_file = self.tmp_file(to_info["path"])
            try:
                ssh.open_sftp().get(
                    from_info["path"], tmp_file, callback=create_cb(name)
                )
            except Exception:
                msg = "failed to download '{}/{}' to '{}'"
                logger.error(
                    msg.format(
                        from_info["host"], from_info["path"], to_info["path"]
                    )
                )
                continue

            os.rename(tmp_file, to_info["path"])
            progress.finish_target(name)

        ssh.close()

    def upload(self, from_infos, to_infos, names=None):
        names = self._verify_path_args(to_infos, from_infos, names)

        ssh = self.ssh(
            host=to_infos[0]["host"],
            user=to_infos[0]["user"],
            port=to_infos[0]["port"],
        )
        sftp = ssh.open_sftp()

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info["scheme"] != "ssh":
                raise NotImplementedError

            if from_info["scheme"] != "local":
                raise NotImplementedError

            logger.debug(
                "Uploading '{}' to '{}/{}'".format(
                    from_info["path"], to_info["host"], to_info["path"]
                )
            )

            if not name:
                name = os.path.basename(from_info["path"])

            dname = posixpath.dirname(to_info["path"])
            self._exec(ssh, "mkdir -p {}".format(dname))

            try:
                sftp.put(
                    from_info["path"],
                    to_info["path"],
                    callback=create_cb(name),
                )
            except Exception:
                msg = "failed to upload '{}' to '{}/{}'"
                logger.error(
                    msg.format(
                        from_info["path"], to_info["host"], to_info["path"]
                    )
                )
                continue

            progress.finish_target(name)

        sftp.close()
        ssh.close()

    def list_cache_paths(self):
        ssh = self.ssh(host=self.host, user=self.user, port=self.port)
        cmd = "find {} -type f -follow -print".format(self.prefix)
        stdout = self._exec(ssh, cmd)
        flist = stdout.split()
        ssh.close()
        return flist
