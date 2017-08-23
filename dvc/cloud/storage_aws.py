import sys, os, argparse, boto.ec2
from dvc.cloud.base_aws import AwsBase #logger, Logger

class StorageTool(AwsBase):

    def __init__(self, homedir, conf_file):
        super().__init__(homedir, conf_file)

    def extend_args(self, parser):
        parser.add_argument('--size', metavar = '', type = int,
                            help = 'Storage volume size in GB. Min=1, Max=16384.')
        parser.add_argument('--snapshot-id', metavar = '',
                            help = 'The snapshot id for the volumne creation.')
        parser.add_argument('--type', metavar = '',
                            help = 'The volume type: standard, io1 - provisioned IOPS, gp2 - general purpose')
        parser.add_argument('--iops', metavar = '',
                            help = 'Number of I\O per second for provisioned IOPS volumes.')
        parser.add_argument('--encryption', metavar = '',
                            help = 'Enable encription for the volume.')
        pass

    def create(self, storage_name):
        if not self._args.size:
            logger.error("Storage volume size is not specified")
            return

        try:
            vol = self._conn.create_volume(self._args.size,
                                           self._args.zone,
                                           self._args.snapshot_id,
                                           self._args.type,
                                           self._args.iops,
                                           self.toBool(self._args.encryption))
            vol.add_tag(self.VOLUME_TAG, storage_name)
        except self._conn.ResponseError as e:
            logger.error('cannot create a volume: %s' % e.message)
            return

        logger.log("%sGB volume %s was created" % (self._args.size, storage_name))
        pass

if __name__ == "__main__":
    HOMEDIR_VAR = "ONEBOXML_HOME"
    if not (HOMEDIR_VAR in os.environ):
        raise Exception(HOMEDIR_VAR + " variable is not defined")
    homedir = os.environ[HOMEDIR_VAR]
    conf_file = homedir + "/oneboxml.conf"

    logger = Logger()

    storage_tool = StorageTool(homedir, conf_file)

    (command, param) = storage_tool.get_command()
    if command == 'create':
        storage_tool.create(param)
