from __future__ import unicode_literals

import errno
import json
import logging
import os

import attr

from dvc.utils.compat import str
from dvc.utils.serializers import json_serializer


logger = logging.getLogger(__name__)


@attr.s
class SystemInfo:
    linux_distro                 = attr.ib(default=None)
    linux_distro_like            = attr.ib(default=None)
    linux_distro_version         = attr.ib(default=None)
    mac_version                  = attr.ib(default=None)
    os                           = attr.ib(default=None)
    windows_version_build        = attr.ib(default=None)
    windows_version_major        = attr.ib(default=None)
    windows_version_minor        = attr.ib(default=None)
    windows_version_service_pack = attr.ib(default=None)

    def collect(self):
        import platform

        system = platform.system().lower()
        f = getattr(self, system)
        f()
        return self

    def windows(self):
        import sys

        version = sys.getwindowsversion()
        self.os = "windows"
        self.windows_version_major = version.major
        self.windows_version_minor = version.minor
        self.windows_version_build = version.build
        self.windows_version_service_pack = version.service_pack

    def darwin(self):
        import platform

        self.os = "mac"
        self.mac_version = platform.mac_ver()[0]

    def linux(self):
        import distro

        self.os = "linux"
        self.linux_distro = distro.id()
        self.linux_distro_version = distro.version()
        self.linux_distro_like = distro.like()


@json_serializer
@attr.s
class UserID:
    import uuid
    from dvc.config import Config

    user_id = attr.ib(default=attr.Factory(uuid.uuid4), converter=str)

    config_dir = Config.get_global_config_dir()
    fname = config_dir / "user_id"

    @classmethod
    def load(cls):
        from dvc.lock import Lock, LockError
        from json import JSONDecodeError

        if not cls.fname.exists:
            return None

        lock = Lock(cls.fname.with_suffix(".lock"))

        try:
            with lock:
                return cls.from_file(cls.fname)

        except ValueError as exc:
            logger.debug("Failed to load user_id: {}".format(exc))

        except JSONDecodeError:
            logger.debug("Failed to read '{}'".format(fname))

        except LockError:
            logger.debug("Failed to acquire '{}'".format(lock.lockfile))

    @classmethod
    def generate(cls):
        from dvc.utils import makedirs

        user_id = UserID()

        makedirs(cls.fname.parent, exist_ok=True)
        user_id.to_file(cls.fname)
        return user_id


@json_serializer
@attr.s
class Report:
    cmd_class       = attr.ib(default=None)
    cmd_return_code = attr.ib(default=None)
    dvc_version     = attr.ib(default=None)
    is_binary       = attr.ib(default=None)
    scm_class       = attr.ib(default=None)
    user_id         = attr.ib(default=None)
    system_info     = attr.ib(default=None)

    def collect(self):
        from dvc import __version__
        from dvc.exceptions import NotDvcRepoError
        from dvc.repo import Repo
        from dvc.scm import SCM
        from dvc.utils import is_binary

        self.dvc_version = __version__
        self.is_binary = is_binary()
        self.user_id = (UserID.load() or UserID.generate()).user_id
        self.system_info = SystemInfo().collect()

        try:
            scm = SCM(root_dir=Repo.find_root())
            self.scm_class = type(scm).__name__
        except NotDvcRepoError:
            pass

    def collect_cmd(self, args, ret):
        if ret:
            self.cmd_return_code = ret

        if args and hasattr(args, "func"):
            self.cmd_class = args.func.__name__


class Analytics(object):
    def __init__(self, report=None):
        self.report = report or Report()

    @staticmethod
    def load(path):
        analytics = Analytics(report=Report.from_file(path))
        os.unlink(path)
        return analytics

    def dump(self):
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False, mode="w") as fobj:
            self.report.to_file(fobj.name)
            return fobj.name

    @staticmethod
    def is_enabled():
        from dvc.config import Config, to_bool
        from dvc.utils import env2bool

        if env2bool("DVC_TEST"):
            return False

        core = Config(validate=False).config.get(Config.SECTION_CORE, {})
        enabled = to_bool(core.get(Config.SECTION_CORE_ANALYTICS, "true"))

        logger.debug(
            "Analytics is {status}."
            .format(status="enabled" if enabled else "disabled")
        )

        return enabled

    @staticmethod
    def send_cmd(cmd, args, ret):
        from dvc.daemon import daemon
        from dvc.command.daemon import CmdDaemonBase

        if not Analytics.is_enabled() or isinstance(cmd, CmdDaemonBase):
            return False

        analytics = Analytics()
        analytics.report.collect_cmd(args, ret)
        daemon(["analytics", analytics.dump()])

    def send(self):
        import requests

        if not self.is_enabled():
            return

        self.report.collect()

        info = self.report.asdict
        url = "https://analytics.dvc.org"

        logger.debug("Sending analytics: {}".format(info))

        try:
            requests.post(url, json=info, timeout=5)
        except requests.exceptions.RequestException as exc:
            logger.debug("Failed to send analytics: {}".format(str(exc)))
