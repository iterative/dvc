import sys, os, time, configparser, argparse, boto.ec2


class AwsBase:
    DEFAULT_REGION = "us-east-1"
    DEFAULT_ZONE = "us-east-1a"

    def __init__(self, homedir, conf_file):
        self._homedir = homedir

        config = configparser.ConfigParser()
        config.read(conf_file)

        self._aws_conf = config['AWS']
        self._args = self.parse_args()

        self._conn = boto.ec2.connect_to_region(
                                    self._args.region,
                                    aws_access_key_id = self._aws_conf.get("AccessKeyID"),
                                    aws_secret_access_key = self._aws_conf.get("SecretAccessKey"))
        pass

    def get_command(self):
        return (self._args.command, self._args.param)

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('command')
        parser.add_argument('param', nargs = '?', default = '')
        parser.add_argument('--region', metavar = '',
                            default = self._aws_conf.get("Region", self.DEFAULT_REGION),
                            help = 'AWS region: us-east-2, ...')
        parser.add_argument('--zone', metavar = '',
                            default = self._aws_conf.get("Zone", self.DEFAULT_ZONE),
                            help = 'Availability Zone. Examples: us-east-1a, us-east-1b, ...')
        self.extend_args(parser)

        args = parser.parse_args()
        return args

    def extend_args(self, parser):
        raise NotImplementedError()

