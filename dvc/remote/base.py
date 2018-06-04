import re

from dvc.config import Config


class RemoteBase(object):
    REGEX = None

    def __init__(self, project, config):
        pass

    @classmethod
    def supported(cls, config):
        url = config[Config.SECTION_REMOTE_URL]
        return cls.match(url) != None

    @classmethod
    def match(cls, url):
        return re.match(cls.REGEX, url)

    def save_info(self, path_info):
        pass

    def save(self, path_info):
        pass

    def checkout(self, path_info, checksum_info):
        pass
