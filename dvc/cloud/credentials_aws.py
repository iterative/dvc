import os
import configparser

from dvc.utils import cached_property
from dvc.logger import Logger


class AWSCredentials(object):
    def __init__(self, cloud_config):
        self._conf_credpath = cloud_config.get('CredentialPath', None)
        self._conf_credsect = cloud_config.get('Profile', 'default')

    @property
    def access_key_id(self):
        if self.creds:
            return self.creds[0]
        return None

    @property
    def secret_access_key(self):
        if self.creds:
            return self.creds[1]
        return None

    @cached_property
    def creds(self):
        return self._get_credentials()

    def _get_credentials(self):
        """ gets aws credentials, looking in various places

        Params:

        Searches:
        1 any override in dvc.conf [AWS] CredentialPath;
        2 ~/.aws/credentials


        Returns:
            if successfully found, (access_key_id, secret)
            None otherwise
        """

        # FIX: It won't work in Windows.
        default_path = os.path.expanduser('~/.aws/credentials')
        default_sect = 'default'
        default_cred_location = (default_path, default_sect)

        cred_locations = self._credential_paths(default_cred_location)
        for cred_location in cred_locations:
            try:
                path = cred_location[0]
                section = cred_location[1]

                cc = configparser.SafeConfigParser()

                # use readfp(open( ... to aid mocking.
                cc.readfp(open(path, 'r'))

                if section in cc.keys():
                    access_key = cc[section].get('aws_access_key_id', None)
                    secret = cc[section].get('aws_secret_access_key', None)

                    if access_key is not None and secret is not None:
                        return (access_key, secret)
                else:
                    Logger.warn('Unable to find section {} in AWS credential file {}'.format(section, path))
            except Exception as e:
                pass

        return None

    def _credential_paths(self, default_cred_location):
        results = []
        if self._conf_credpath is not None and len(self._conf_credpath) > 0:
            credpath = os.path.expanduser(self._conf_credpath)
            if os.path.isfile(credpath):
                results.append((credpath, self._conf_credsect))
            else:
                msg = 'AWS CredentialPath {} not found; falling back to default file {} and section {}'
                Logger.warn(msg.format(credpath, default_cred_location[0], default_cred_location[1]))
                results.append(default_cred_location)
        else:
            results.append(default_cred_location)
        return results

    def sanity_check(self):
        creds = self._get_credentials()
        if creds is None:
            Logger.info("can't find aws credetials, assuming envirment variables or iam role")
        # self._aws_creds = creds
