import os
import json
import errno

from dvc import VERSION
from dvc.logger import Logger


class Analytics(object):
    URL = 'https://dddc07bh9f.execute-api.us-east-2.amazonaws.com/production'
    TIMEOUT_POST = 5

    USER_ID_FILE = 'user_id'

    PARAM_DVC_VERSION = 'dvc_version'
    PARAM_USER_ID = 'user_id'
    PARAM_SYSTEM_INFO = 'system_info'

    PARAM_OS = 'os'

    PARAM_WINDOWS_VERSION_MAJOR = 'windows_version_major'
    PARAM_WINDOWS_VERSION_MINOR = 'windows_version_minor'
    PARAM_WINDOWS_VERSION_BUILD = 'windows_version_build'
    PARAM_WINDOWS_VERSION_SERVICE_PACK = 'windows_version_service_pack'

    PARAM_MAC_VERSION = 'mac_version'

    PARAM_LINUX_DISTRO = 'linux_distro'
    PARAM_LINUX_DISTRO_VERSION = 'linux_distro_version'
    PARAM_LINUX_DISTRO_LIKE = 'linux_distro_like'

    PARAM_SCM_CLASS = 'scm_class'
    PARAM_IS_BINARY = 'is_binary'
    PARAM_CMD_CLASS = 'cmd_class'
    PARAM_CMD_RETURN_CODE = 'cmd_return_code'

    def __init__(self, info={}):
        from dvc.config import Config
        from dvc.lock import Lock

        self.info = info

        cdir = Config.get_global_config_dir()
        try:
            os.makedirs(cdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        self.user_id_file = os.path.join(cdir, self.USER_ID_FILE)
        self.user_id_file_lock = Lock(cdir, self.USER_ID_FILE + '.lock')

    @staticmethod
    def load(path):
        with open(path, 'r') as fobj:
            a = Analytics(info=json.load(fobj))
        os.unlink(path)
        return a

    def _write_user_id(self):
        import uuid

        with open(self.user_id_file, 'w+') as fobj:
            user_id = str(uuid.uuid4())
            info = {self.PARAM_USER_ID: user_id}
            json.dump(info, fobj)
            return user_id

    def _read_user_id(self):
        if not os.path.exists(self.user_id_file):
            return None

        with open(self.user_id_file, 'r') as fobj:
            try:
                info = json.load(fobj)
                return info[self.PARAM_USER_ID]
            except json.JSONDecodeError as exc:
                Logger.debug("Failed to load user_id: {}".format(exc))
                return None

    def _get_user_id(self):
        from dvc.lock import LockError

        try:
            with self.user_id_file_lock:
                user_id = self._read_user_id()
                if user_id is None:
                    user_id = self._write_user_id()
                return user_id
        except LockError:
            msg = "Failed to acquire '{}'"
            Logger.debug(msg.format(self.user_id_file_lock.lock_file))

    def _collect_windows(self):
        import sys
        version = sys.getwindowsversion()
        info = {}
        info[self.PARAM_OS] = 'windows'
        info[self.PARAM_WINDOWS_VERSION_MAJOR] = version.major
        info[self.PARAM_WINDOWS_VERSION_MINOR] = version.minor
        info[self.PARAM_WINDOWS_VERSION_BUILD] = version.build
        info[self.PARAM_WINDOWS_VERSION_SERVICE_PACK] = version.service_pack
        return info

    def _collect_darwin(self):
        import platform
        info = {}
        info[self.PARAM_OS] = 'mac'
        info[self.PARAM_MAC_VERSION] = platform.mac_ver()[0]
        return info

    def _collect_linux(self):
        import distro
        info = {}
        info[self.PARAM_OS] = 'linux'
        info[self.PARAM_LINUX_DISTRO] = distro.id()
        info[self.PARAM_LINUX_DISTRO_VERSION] = distro.version()
        info[self.PARAM_LINUX_DISTRO_LIKE] = distro.like()
        return info

    def _collect_system_info(self):
        import platform
        system = platform.system()
        if system == 'Windows':
            return self._collect_windows()
        elif system == 'Darwin':
            return self._collect_darwin()
        elif system == 'Linux':
            return self._collect_linux()
        else:
            raise NotImplementedError

    def collect(self):
        from dvc.scm import SCM
        from dvc.utils import is_binary
        from dvc.project import Project
        from dvc.exceptions import NotDvcProjectError

        self.info[self.PARAM_DVC_VERSION] = VERSION
        self.info[self.PARAM_IS_BINARY] = is_binary()
        self.info[self.PARAM_USER_ID] = self._get_user_id()

        self.info[self.PARAM_SYSTEM_INFO] = self._collect_system_info()

        try:
            scm = SCM(root_dir=Project._find_root())
            self.info[self.PARAM_SCM_CLASS] = type(scm).__name__
        except NotDvcProjectError:
            pass

    def collect_cmd(self, args, ret):
        from dvc.command.daemon import CmdDaemonAnalytics

        assert isinstance(ret, int) or ret is None

        if ret is not None:
            self.info[self.PARAM_CMD_RETURN_CODE] = ret

        if args is not None and hasattr(args, 'func'):
            assert args.func != CmdDaemonAnalytics
            self.info[self.PARAM_CMD_CLASS] = args.func.__name__

    def dump(self):
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False, mode='w') as fobj:
            json.dump(self.info, fobj)
            return fobj.name

    @staticmethod
    def _is_enabled(cmd=None):
        from dvc.config import Config
        from dvc.project import Project
        from dvc.exceptions import NotDvcProjectError
        from dvc.command.daemon import CmdDaemonBase

        if os.getenv('DVC_TEST'):
            return False

        if isinstance(cmd, CmdDaemonBase):
            return False

        if cmd is None or not hasattr(cmd, 'config'):
            try:
                dvc_dir = Project._find_dvc_dir()
                config = Config(dvc_dir)
                assert config is not None
            except NotDvcProjectError:
                config = Config(validate=False)
                assert config is not None
        else:
            config = cmd.config
            assert config is not None

        core = config._config.get(Config.SECTION_CORE, {})
        enabled = core.get(Config.SECTION_CORE_ANALYTICS, True)
        Logger.debug("Analytics is {}abled."
                     .format('en' if enabled else 'dis'))
        return enabled

    @staticmethod
    def send_cmd(cmd, args, ret):
        from dvc.daemon import Daemon

        if not Analytics._is_enabled(cmd):
            return

        a = Analytics()
        a.collect_cmd(args, ret)
        Daemon()(['analytics', a.dump()])

    def send(self):
        import requests

        if not self._is_enabled():
            return

        self.collect()

        Logger.debug("Sending analytics: {}".format(self.info))

        try:
            requests.post(self.URL, json=self.info, timeout=self.TIMEOUT_POST)
        except requests.exceptions.RequestException as exc:
            Logger.debug("Failed to send analytics: {}".format(str(exc)))
