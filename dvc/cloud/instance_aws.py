import sys, os, time, boto.ec2

from dvc.cloud.credentials_aws import AWSCredentials
from dvc.cloud.instance import Instance, InstanceError
from dvc.logger import Logger


class InstanceAws(Instance):
    TERMINATED_STATE = 'terminated'
    INSTANCE_FORMAT = "{:8s}{:12s}{:12s}{:12s}{:16s}{:16s}{:16s}"
    VOLUME_FORMAT = "{:20s}{:16s}{:12s}{:12s}{:12s}{:12s}{:16s}"

    def __init__(self, homedir, conf_file):
        super(InstanceAws).__init__(homedir, conf_file)
        self._aws_creds = AWSCredentials(self._cloud_config)

        self._conn = boto.ec2.connect_to_region(
                                    self._region,
                                    aws_access_key_id=self._aws_conf.get("AccessKeyID"),
                                    aws_secret_access_key=self._aws_conf.get("SecretAccessKey"))

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
        (volumes, rest_volumes) = self.all_volumes()

        for volume in volumes:
            if volume.tags[self.VOLUME_TAG] == self._storage:
                return volume
        msg = u'Cannot find storage volume {}. Verify that the volume was created.'.format(
            self._storage)
        raise InstanceError(msg)

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
        Logger.info(u'Waiting for a spot instance. Request {}.'.format(req[0].id))
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
            raise InstanceError(u'Unable to obtain {} spot instance in region {} for price ${}: {}'.format(
                            (self._args.instance_type, self._args.region, self._args.spot_price,
                             'the request was canceled')))

        Logger.info(u'{} spot instance was created: {}'.format(self._type, job_instance_id))
        reservations = self._conn.get_all_instances(instance_ids = job_instance_id)
        instance = reservations[0].instances[0]
        return instance

    def run_instance(self):
        if not self._image:
            raise InstanceError('Cannot run EC2 instance: image (AMI) is not defined')

        instance = self.create_instance()

        # Remove active tag.
        active_filter = {'tag-key': self.INSTANCE_STATE_TAG, 'tag-value': 'True'}
        active_reserv = self._conn.get_all_instances(filters = active_filter)
        active_instances = [i for r in active_reserv for i in r.instances]

        if len(active_instances) > 0:
            #active_insts = active_reserv.instances
            if len(active_instances) > 1:
                Logger.error('EC2 instances consistency error - more than one active EC2 instance')
            for inst in active_instances:
                inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
                inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
                if inst.state != self.TERMINATED_STATE:
                    Logger.log('{} instance {} is not longer active'.format(inst.instance_type, inst.id))

        # Assign the created instace as active.
        instance.add_tag(self.INSTANCE_STATE_TAG, 'True')
        Logger.info('New {} instance {} was selected as active'.format(instance.instance_type, instance.id))

        Logger.info('Waiting for a running status')
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
                Logger.error('AWS Security Group {} does not exist: creating the group'.format(group_name))
                group = self._conn.create_security_group(group_name, 'group with SSH access')
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
            raise InstanceError('AWS keypair cannot be created: KeyName is not specified in AWS section in the config file')
        if not self._keypair_dir:
            raise InstanceError('AWS keypair cannot be created: KeyDir is not specified in AWS section in the config file')

        # Check if the key exists and create one if does not.
        try:
            key = self._conn.get_all_key_pairs(keynames=[self._keypair_name])[0]
        except self._conn.ResponseError as e:
            if e.code == 'InvalidKeyPair.NotFound':
                Logger.info('AWS key {} does not exist: creating the key'.format(self._keypair_name))
                # Create an SSH key to use when logging into instances.
                key = self._conn.create_key_pair(self._keypair_name)
                Logger.info('AWS key was created: {}'.format(self._keypair_name))

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
                Logger.info('AWS private key file was saved: {}'.format(key_file))
            else:
                raise

        return self._keypair_name

    def all_instances(self, terminated = False):
        active = []
        not_active = []
        rest_inst = []

        reserv = self._conn.get_all_instances()
        instances = [i for r in reserv for i in r.instances]
        for inst in instances:
            if terminated == False and inst.state == self.TERMINATED_STATE:
                continue

            if self.INSTANCE_STATE_TAG in inst.tags:
                if inst.tags[self.INSTANCE_STATE_TAG] == 'True':
                    active.append(inst)
                else:
                    not_active.append(inst)
            else:
                rest_inst.append(inst)

        if len(active) > 1:
            Logger.error(u'Instance consistancy error - more than one instance is in active state')
        if len(active) == 0 and len(not_active) > 0:
            Logger.error(u'Instance consistancy error - no active instances')
        return (active, not_active, rest_inst)

    def describe_instance(self, inst, volumes, is_active):
        ip = inst.ip_address
        ip_private = inst.private_ip_address
        if not ip:
            ip = ''
        if not ip_private:
            ip_private = ''

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
                        Logger.error('Instance {} storage volume does not have tag'.format(inst.id))
                    break

        return self.INSTANCE_FORMAT.format(activeFlag,
                                           inst.id[:12],
                                           inst.instance_type[:12],
                                           inst.state[:12],
                                           volume_name,
                                           ip[:16],
                                           ip_private[:16])

    def describe_instances(self):
        (active, not_active, rest_inst) = self.all_instances()
        (volumes, rest_volumes) = self.all_volumes()

        Logger.info(self.INSTANCE_FORMAT.format('Active', 'Id', 'Type', 'State', 'Storage',
                                                'IP public', 'IP private'))

        for inst in active:
            Logger.info(self.describe_instance(inst, volumes, True))
        for inst in not_active:
            logger.info(self.describe_instance(inst, volumes, False))

        if len(rest_inst) > 0:
            Logger.error(u'{} not tracked instances were found: not in the list'.format(len(rest_inst)))
        pass

    def get_instance_id(self, instance):
        if instance[:2] != 'i-' and len(instance) == 8:
            instance = 'i-' + instance
        return instance

    def terminate_instances(self, instance):
        instance = self.get_instance_id(instance)

        if not instance:
            Logger.error('Instance Id is not specified')
            return

        (active, not_active, rest_inst) = self.all_instances()

        if instance == 'all':
            target_in_active = active
            target_in_not_active = not_active
        else:
            target_in_active = list(filter(lambda inst: inst.id == instance, active))
            target_in_not_active = list(filter(lambda inst: inst.id == instance, not_active))

        if target_in_not_active:
            self._conn.terminate_instances(instance_ids = [inst.id for inst in target_in_not_active])

        for inst in target_in_active:
            inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
            inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
            self._conn.terminate_instances(instance_ids = [inst.id])

        if instance != 'all' and len(target_in_active) > 0 and len(not_active) > 0:
            new_active_inst = not_active[0]
            new_active_inst.remove_tag(self.INSTANCE_STATE_TAG, 'False')
            new_active_inst.add_tag(self.INSTANCE_STATE_TAG, 'True')
            randomly = ''
            if len(not_active) > 1:
                randomly = 'randomly '
            Logger.error('{} instance {} was {} selected as active because of an active instance was terminated'.format(
                        new_active_inst.instance_type, new_active_inst.id, randomly))
        pass

    def set_active_instance(self, instance):
        instance = self.get_instance_id(instance)

        if not instance:
            Logger.error('Instance Id is not specified')
            return

        (active, not_active, rest_inst) = self.all_instances()

        target_in_active = list(filter(lambda inst: inst.id == instance, active))
        if len(target_in_active) > 0:
            Logger.error('The instance is already active')
            return

        target_in_not_actives = list(filter(lambda inst: inst.id == instance, not_active))
        if len(target_in_not_actives) == 0:
            Logger.error('The instance is not running')
            return

        if len(target_in_not_actives) > 1:
            Logger.error('Instances consistancy error: more than one instance with the same id')
            return

        target_in_not_actives[0].remove_tag(self.INSTANCE_STATE_TAG, 'False')
        target_in_not_actives[0].add_tag(self.INSTANCE_STATE_TAG, 'True')

        for inst in active:
            inst.remove_tag(self.INSTANCE_STATE_TAG, 'True')
            inst.add_tag(self.INSTANCE_STATE_TAG, 'False')
        pass

    def all_volumes(self):
        rest_volumes = []
        volumes = self._conn.get_all_volumes()
        for vol in volumes:
            if self.VOLUME_TAG in vol.tags:
                volumes.append(vol)
            else:
                rest_volumes.append(vol)
        return (volumes, rest_volumes)

    def describe_storages(self):
        Logger.info(self.VOLUME_FORMAT.format('Name', 'Id', 'Size(GB)',
                                              'Status', 'Type', 'Encrypted', 'Zone'))
        (volumes, rest_volumes) = self.all_volumes()
        for vol in volumes:
            Logger.info(self.VOLUME_FORMAT.format(vol.tags[self.VOLUME_TAG],
                                                  vol.id, str(vol.size), vol.status,
                                                  vol.type, str(vol.encrypted), vol.zone))
        pass
