import sys, os, time, configparser, argparse, boto.ec2
from base_aws import AwsBase

from dvc.cloud.instance import Instance


class InstanceAws(Instance):
    TERMINATED_STATE = 'terminated'
    INSTANCE_FORMAT = "{:8s}{:12s}{:12s}{:12s}{:16s}{:16s}{:16s}"
    VOLUME_FORMAT = "{:20s}{:16s}{:12s}{:12s}{:12s}{:12s}{:16s}"

    def __init__(self, homedir, conf_file):
        super(InstanceAws).__init__(homedir, conf_file)

        self._conn = boto.ec2.connect_to_region(
                                    self._region,
                                    aws_access_key_id = self._aws_conf.get("AccessKeyID"),
                                    aws_secret_access_key = self._aws_conf.get("SecretAccessKey"))

    def create(self):
        instance = None

        if not self._spot_price:
            reserv = self._conn.run_instances(self._image,
                                              key_name=self.get_key_name(),
                                              instance_type=self._type,
                                              security_groups=self.get_security_groups(),
                                              monitoring_enabled=self.toBool(self._monitoring),
                                              #subnet_id = self._args.subnet_id,
                                              placement=self._zone,
                                              ebs_optimized=self.toBool(self._ebs_optimized))
            instance = reserv.instances[0]
        else:
            instance = self.create_spot_instance()

        return instance

    def find_volume(self):
        (oneboxml_volume, rest_volumes) = self.all_volumes()

        for volume in oneboxml_volume:
            if  volume.tags[self.VOLUME_TAG] == self._args.storage:
                return volume
        raise ValueError("Cannot find OneBoxML storage volume '%s'. Verify that the volume was created."
                         % self._args.storage)

    def create_spot_instance(self):
        # Create spot instance
        req = self._conn.request_spot_instances(price = self._args.spot_price,
                                                image_id = self._args.image,
                                                key_name = self.get_key_name(),
                                                instance_type = self._args.instance_type,
                                                security_groups = self.get_security_groups(),
                                                monitoring_enabled = self.toBool(self._args.monitoring),
                                                #subnet_id = self._args.subnet_id,
                                                placement = self._args.zone,
                                                ebs_optimized = self.toBool(self._args.ebs_optimized))
        job_instance_id = None
        sec = 0
        sys.stdout.write("Waiting for a spot instance. Request %s." % req[0].id)
        while job_instance_id == None and sec < self._args.spot_timeout:
            sys.stdout.write(".")
            sys.stdout.flush()

            job_sir_id = req[0].id
            reqs = self._conn.get_all_spot_instance_requests()
            for sir in reqs:
                if sir.id == job_sir_id:
                    job_instance_id = sir.instance_id
                    break
            time.sleep(1)
            sec += 1
        sys.stdout.write("\n")

        if not job_instance_id:
            self._conn.cancel_spot_instance_requests(req[0].id)
            msg = "the request was canceled"
            raise Exception("Unable to obtain %s spot instance in region %s for price $%s: %s" %
                            (self._args.instance_type, self._args.region, self._args.spot_price, msg))

        logger.log("%s spot instance was created: %s" % (self._args.instance_type, job_instance_id))
        reservations = self._conn.get_all_instances(instance_ids = job_instance_id)
        instance = reservations[0].instances[0]
        return instance

    def run_instance(self):
        if self._args.image == None or self._args.image == '':
            raise Exception("Cannot run EC2 instance: image (AMI) is not defined")

        instance = self.create_instance()

        # Remove oneboxml active tag.
        active_filter = {'tag-key': self.INSTANCE_STATE_TAG, 'tag-value': 'True'}
        active_reserv = self._conn.get_all_instances(filters = active_filter)
        active_instances = [i for r in active_reserv for i in r.instances]

        if len(active_instances) > 0:
            #active_insts = active_reserv.instances
            if len(active_instances) > 1:
                logger.error("EC2 instances consistency error - more than one active EC2 instance")
            for inst in active_instances:
                inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
                inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
                if inst.state != self.TERMINATED_STATE:
                    logger.log("%s instance %s is not longer active" % (inst.instance_type, inst.id))

        # Assign the created instace as active.
        instance.add_tag(self.INSTANCE_STATE_TAG, 'True')
        logger.log("New %s instance %s was selected as active"%(instance.instance_type, instance.id))

        sys.stdout.write("Waiting for a running status")
        while instance.state != 'running':
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(1)
            instance.update()
        sys.stdout.write("\n")

        self._conn.attach_volume(self.find_volume().id, instance.id, "/dev/sdx")
        pass

    def get_security_groups(self, ssh_port = 22):
        group_name = self._security_group

        # Check if the group exists and create one if does not.
        try:
            group = self._conn.get_all_security_groups(groupnames=[group_name])[0]
        except self._conn.ResponseError as e:
            if e.code == 'InvalidGroup.NotFound':
                logger.log('AWS Security Group %s does not exist: creating the group' % group_name)
                group = self._conn.create_security_group(group_name, 'OneBoxML group with SSH access')
            else:
                raise

        # Enable SSH access.
        try:
            group.authorize('tcp', ssh_port, ssh_port, '0.0.0.0/0')
        except self._conn.ResponseError as e:
            if e.code != 'InvalidPermission.Duplicate':
                raise

        return [group_name]

    def get_key_name(self):
        if not self._keypair_name:
            raise Exception("AWS keypair cannot be created: KeyName is not specified in AWS section in the config file")
        if not self._keypair_dir:
            raise Exception("AWS keypair cannot be created: KeyDir is not specified in AWS section in the config file")

        # Check if the key exists and create one if does not.
        try:
            key = self._conn.get_all_key_pairs(keynames = [self._keypair_name])[0]
        except self._conn.ResponseError as e:
            if e.code == 'InvalidKeyPair.NotFound':
                logger.log('AWS key %s does not exist: creating the key' % self._keypair_name)
                # Create an SSH key to use when logging into instances.
                key = self._conn.create_key_pair(self._keypair_name)
                logger.log("AWS key was created: " + self._keypair_name)

                # Expand key dir.
                key_dir = os.path.expandvars(self._keypair_dir)
                if not os.path.isdir(self._keypair_dir):
                    os.mkdir(self._keypair_dir, 0o700)

                #  Private key has to be stored locally.
                key_file = os.path.join(key_dir, self._keypair_name + '.pem')
                #key.save(key_file) # doesn't work in python3
                fp = open(key_file, 'w')
                fp.write(key.material)
                fp.close()
                os.chmod(key_file, 0o600)
                logger.log("AWS private key file was saved: " + key_file)
            else:
                raise

        return self._keypair_name

    def all_instances(self, terminated = False):
        oneboxml_active = []
        oneboxml_not_active = []
        rest_inst = []

        reserv = self._conn.get_all_instances()
        instances = [i for r in reserv for i in r.instances]
        for inst in instances:
            if terminated == False and inst.state == self.TERMINATED_STATE:
                continue

            if self.INSTANCE_STATE_TAG in inst.tags:
                if inst.tags[self.INSTANCE_STATE_TAG] == 'True':
                    oneboxml_active.append(inst)
                else:
                    oneboxml_not_active.append(inst)
            else:
                rest_inst.append(inst)

        if len(oneboxml_active) > 1:
            logger.error("Instance consistancy error - more than one instance is in active state")
        if len(oneboxml_active) == 0 and len(oneboxml_not_active) > 0:
            logger.error("Instance consistancy error - no active instances")
        return (oneboxml_active, oneboxml_not_active, rest_inst)

    def describe_instance(self, inst, volumes, is_active):
        ip = inst.ip_address
        ip_private = inst.private_ip_address
        if not ip:
            ip = ""
        if not ip_private:
            ip_private = ""

        activeFlag = ''
        if is_active:
            activeFlag = ' ***'


        volume_id = None
        volume_name = ''
        bdm = inst.block_device_mapping
        for device_type in bdm.values():
            for vol in volumes:
                if device_type.volume_id == vol.id:
                    if self.VOLUME_TAG in vol.tags:
                        volume_name = vol.tags[self.VOLUME_TAG]
                    else:
                        log.error("Instance %s storage volume doesn't have tag", inst.id)
                    break

        return self.INSTANCE_FORMAT.format(activeFlag,
                                           inst.id[:12],
                                           inst.instance_type[:12],
                                           inst.state[:12],
                                           volume_name,
                                           ip[:16],
                                           ip_private[:16])

    def describe_instances(self):
        (oneboxml_active, oneboxml_not_active, rest_inst) = self.all_instances()
        (oneboxml_volumes, rest_volumes) = self.all_volumes()

        logger.log(self.INSTANCE_FORMAT.format("Active", "Id", "Type", "State", "Storage",
                                               "IP public", "IP private"))


        for inst in oneboxml_active:
            logger.log(self.describe_instance(inst, oneboxml_volumes, True))
        for inst in oneboxml_not_active:
            logger.log(self.describe_instance(inst, oneboxml_volumes, False))

        if len(rest_inst) > 0:
            logger.warn("%s addition not OneBoxML instances were found: not in the list"\
                        % len(rest_inst))
        pass

    def get_instance_id(self, instance):
        if instance[:2] != 'i-' and len(instance) == 8:
            instance = 'i-' + instance
        return instance

    def terminate_instances(self, instance):
        instance = self.get_instance_id(instance)

        if not instance:
            logger.error("Instance Id is not specified")
            return

        (oneboxml_active, oneboxml_not_active, rest_inst) = self.all_instances()

        if instance == 'all':
            target_in_active = oneboxml_active
            target_in_not_active = oneboxml_not_active
        else:
            target_in_active = list(filter(lambda inst: inst.id == instance, oneboxml_active))
            target_in_not_active = list(filter(lambda inst: inst.id == instance, oneboxml_not_active))

        if target_in_not_active:
            self._conn.terminate_instances(instance_ids = [inst.id for inst in target_in_not_active])

        for inst in target_in_active:
            inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
            inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
            self._conn.terminate_instances(instance_ids = [inst.id])

        if instance != 'all' and len(target_in_active) > 0 and len(oneboxml_not_active) > 0:
            new_active_inst = oneboxml_not_active[0]
            new_active_inst.remove_tag(self.INSTANCE_STATE_TAG, 'False')
            new_active_inst.add_tag(self.INSTANCE_STATE_TAG, 'True')
            randomly = ''
            if len(oneboxml_not_active) > 1:
                randomly = 'randomly '
            logger.warn("%s instance %s was %sselected as active because of an active instance was terminated" %
                        (new_active_inst.instance_type, new_active_inst.id, randomly))
        pass

    def set_active_instance(self, instance):
        instance = self.get_instance_id(instance)

        if not instance:
            logger.error("Instance Id is not specified")
            return

        (oneboxml_active, oneboxml_not_active, rest_inst) = self.all_instances()

        target_in_active = list(filter(lambda inst: inst.id == instance, oneboxml_active))
        if len(target_in_active) > 0:
            logger.error("The instance is already active")
            return

        target_in_not_actives = list(filter(lambda inst: inst.id == instance, oneboxml_not_active))
        if len(target_in_not_actives) == 0:
            logger.error("The instance is not running")
            return

        if len(target_in_not_actives) > 1:
            logger.error("Instances consistancy error: more than one instance with the same id")
            return

        target_in_not_actives[0].remove_tag(self.INSTANCE_STATE_TAG, 'False')
        target_in_not_actives[0].add_tag(self.INSTANCE_STATE_TAG, 'True')

        for inst in oneboxml_active:
            inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
            inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
        pass

    def all_volumes(self):
        oneboxml_volumes = []
        rest_volumes = []
        volumes = self._conn.get_all_volumes()
        for vol in volumes:
            if self.VOLUME_TAG in vol.tags:
                oneboxml_volumes.append(vol)
            else:
                rest_volumes.append(vol)
        return (oneboxml_volumes, rest_volumes)


    def describe_storages(self):
        logger.log(self.VOLUME_FORMAT.format("Name", "Id", "Size(GB)",
                                             "Status", "Type", "Encrypted", "Zone"))
        (oneboxml_volume, rest_volumes) = self.all_volumes()
        for vol in oneboxml_volume:
            logger.log(self.VOLUME_FORMAT.format(vol.tags[self.VOLUME_TAG],
                                                 vol.id, str(vol.size), vol.status,
                                                 vol.type, str(vol.encrypted), vol.zone))
        pass

if __name__ == "__main__":
    HOMEDIR_VAR = "ONEBOXML_HOME"
    if not (HOMEDIR_VAR in os.environ):
        raise Exception(HOMEDIR_VAR + " variable is not defined")
    homedir = os.environ[HOMEDIR_VAR]
    conf_file = homedir + "/oneboxml.conf"

    logger = Logger()

    instance_tool = InstanceAws(homedir, conf_file)

    (command, param) = instance_tool.get_command()
    if command == 'run':
        instance_tool.run_instance()
    if command == 'describe-instances':
        instance_tool.describe_instances()
    if command == 'terminate-instances':
        instance_tool.terminate_instances(param)
    if command == 'set-active-instance':
        instance_tool.set_active_instance(param)

    if command == 'describe-storages':
        instance_tool.describe_storages()
