from dvc.exceptions import DvcException


class InstanceError(DvcException):
    def __init__(self, msg):
        super(InstanceError).__init__(u"Instance error: " + msg)


class Instance(object):
    INSTANCE_STATE_TAG = 'dvc-is-active'
    VOLUME_TAG = 'dvc-volume'

    def __init__(self, name, cloud, parsed_args, conf_parser):
        self._name = name
        self._cloud = cloud

        self._type = parsed_args.type or conf_parser._config[cloud]['Type']
        self._image = parsed_args.image or conf_parser._config[cloud]['Image']

        self._spot_price = parsed_args.spot_price or conf_parser._config[cloud]['SpotPrice']
        self._spot_timeout = parsed_args.spot_timeout or conf_parser._config[cloud]['SpotTimeout']

        self._keypair_name = parsed_args.keypair_name or conf_parser._config[cloud]['KeyPairName']
        self._keypair_dir = parsed_args.keypair_dir or conf_parser._config[cloud]['KeyPairDir']
        self._security_group = parsed_args.security_group or conf_parser._config[cloud]['SecurityGroup']

        self._region = parsed_args.region or conf_parser._config[cloud]['Region']
        self._zone = parsed_args.zone or conf_parser._config[cloud]['Zone']
        self._subnet_id = parsed_args.subnet_id or conf_parser._config[cloud]['SubnetId']

        self._storage = parsed_args.storage or conf_parser._config[cloud]['Storage']

        self._monitoring = parsed_args.monitoring or conf_parser._config[cloud]['Monitoring']
        self._ebs_optimized = parsed_args.ebs_optimized or conf_parser._config[cloud]['EbsOptimized']
        self._disks_to_raid0 = parsed_args.disks_to_ride0 or conf_parser._config[cloud]['DisksToRAID0']
        pass

    def toBool(self, value):
        if value == None:
            return False
        return value.lower() in ("yes", "true", "t", "1")