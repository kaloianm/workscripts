'''Common EC2 instance launching and management utilities.'''

import copy
import json
import logging
import os

#
# Host configuration scripts. Their output will show up in /var/log/cloud-init-output.log.
#

ANY_HOST_CONFIGURATION = '''#!/bin/bash
set -e

###################################################################################################
echo "Applying OS configuration"
###################################################################################################

sudo bash -c "echo '# BEGIN USER ULIMITS BLOCK' >> /etc/security/limits.conf"
sudo bash -c "echo '$DEFAULT_USER hard nofile 64000' >> /etc/security/limits.conf"
sudo bash -c "echo '$DEFAULT_USER hard nproc 64000' >> /etc/security/limits.conf"
sudo bash -c "echo '$DEFAULT_USER soft nofile 64000' >> /etc/security/limits.conf"
sudo bash -c "echo '$DEFAULT_USER soft nproc 64000' >> /etc/security/limits.conf"
sudo bash -c "echo '# END USER ULIMITS BLOCK' >> /etc/security/limits.conf"
sudo bash -c 'echo "vm.max_map_count = 262144" >> /etc/sysctl.conf'
sudo sysctl -p

###################################################################################################
echo "Configuring required packages"
###################################################################################################

sudo apt update -y
sudo apt install -y vim build-essential dstat sysstat

sudo -u ubuntu \
    NONINTERACTIVE=1 \
    bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

sudo -u ubuntu bash -c 'echo >> /home/ubuntu/.bashrc'
sudo -u ubuntu bash -c 'echo '"'"'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"'"'"' >> /home/ubuntu/.bashrc'

sudo -u ubuntu bash -c 'echo >> /home/ubuntu/.bash_profile'
sudo -u ubuntu bash -c 'echo '"'"'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"'"'"' >> /home/ubuntu/.bash_profile'

eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"

sudo -u ubuntu -i brew update
sudo -u ubuntu -i brew install python3 mongosh

sudo -u ubuntu -i git clone --single-branch https://github.com/kaloianm/workscripts.git /home/ubuntu/workscripts
'''

CLIENT_HOST_TEMPLATE = os.path.join(os.path.dirname(__file__), '..', 'ClientHost.json')


def make_client_driver_host_configuration(clustertag):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring driver host workscripts for {clustertag}"
###################################################################################################
'''


def make_cluster_host_configuration(clustertag, filesystem, skip_format=False):
    format_script = '' if skip_format else f'''
if [ ! -e "/dev/nvme1n1p1" ]; then
  echo "Partitioning data volume ..."
  sudo parted -s /dev/nvme1n1 mklabel gpt &&
    sudo parted -s -a optimal /dev/nvme1n1 mkpart primary 0% 100%

  echo "Making {filesystem} filesystem ..."
  while [ ! -e "/dev/nvme1n1p1" ]; do sleep 1; done
  sudo mkfs -t {filesystem} /dev/nvme1n1p1
fi
'''

    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring shard host volumes for {clustertag}"
###################################################################################################

echo "Waiting for volume to be attached ..."
while [ ! -e "/dev/nvme1n1" ]; do sleep 1; done
{format_script}
echo "Mounting data volume ..."
sudo mkdir -p /mnt/data
sudo mount /dev/nvme1n1p1 /mnt/data
sudo chown -R ubuntu:ubuntu /mnt/data

echo "Data volume mounted, persisting mount point so it survives reboots ..."
if [ -z $(grep "/mnt/data" "/etc/fstab") ]; then echo $(cat "/proc/mounts" | grep "/mnt/data") >> /etc/fstab; fi

echo "Completed configuration for {clustertag} !"
'''


def make_instance_tag_specifications(clustertag, role):
    '''Instantiates an AWS instance tag specification for the specified cluster node'''

    return [{
        'ResourceType':
            'instance',
        'Tags': [{
            'Key': 'mongo_ctools_cluster',
            'Value': clustertag
        }, {
            'Key': 'mongoversion',
            'Value': clustertag
        }, {
            'Key': 'mongorole',
            'Value': role
        }, {
            'Key': 'noreap',
            'Value': 'true'
        }]
    }]


def load_template(template_path):
    '''Loads a JSON template file and returns its contents as kwargs for run_instances'''
    with open(template_path) as f:
        template = json.load(f)

    # MinCount/MaxCount are controlled by the caller, not the template
    template.pop('MinCount', None)
    template.pop('MaxCount', None)

    return template


def launch_instances(ec2, template, tag_specs, user_data, count):
    '''Launches EC2 instances using the template as base parameters'''

    # Merge tag specifications from the template with the caller's tags
    merged_tag_specs = list(template.get('TagSpecifications', []))
    for spec in tag_specs:
        existing = next((s for s in merged_tag_specs if s['ResourceType'] == spec['ResourceType']),
                        None)
        if existing:
            existing['Tags'] = existing['Tags'] + spec['Tags']
        else:
            merged_tag_specs.append(spec)

    params = {k: v for k, v in template.items() if k != 'TagSpecifications'}
    return ec2.run_instances(
        **params,
        TagSpecifications=merged_tag_specs,
        UserData=user_data,
        MinCount=count,
        MaxCount=count,
    )['Instances']


def wait_for_instances(ec2, instances):
    '''Waits for the given instances to reach the running state'''
    instance_ids = [i['InstanceId'] for i in instances]
    logging.info(f'Waiting for {len(instance_ids)} instances to start running ...')
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=instance_ids)


def describe_all_instances(ec2, clustertag):
    '''Issues a query to describe all running instances with the specified cluster tag'''

    filters = [{
        'Name': 'tag:owner',
        'Values': ['kaloian.manassiev']
    }, {
        'Name': 'instance-state-name',
        'Values': ['running']
    }]

    if clustertag:
        filters.append({'Name': 'tag:mongoversion', 'Values': [clustertag]})

    response = ec2.describe_instances(Filters=filters)

    assert ('NextToken' not in response)
    all_instances = []
    for reservation in response['Reservations']:
        all_instances += reservation['Instances']

    return all_instances


def filter_instances_by_role(instances, role):
    '''Filters a list of instances by their mongorole tag'''
    return list(filter(lambda x: {
        'Key': 'mongorole',
        'Value': role,
    } in x['Tags'], instances))


def terminate_cluster_resources(ec2, clustertag):
    '''Terminates all instances and deletes all EBS volumes tagged with the given cluster tag'''

    tag_filter = [{'Name': 'tag:mongo_ctools_cluster', 'Values': [clustertag]}]

    # Find and terminate instances
    instance_filter = tag_filter + [{
        'Name': 'instance-state-name',
        'Values': ['running', 'stopped']
    }]
    response = ec2.describe_instances(Filters=instance_filter)
    instance_ids = []
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_ids.append(instance['InstanceId'])

    if instance_ids:
        logging.info(f'Terminating {len(instance_ids)} instance(s): {instance_ids}')
        ec2.terminate_instances(InstanceIds=instance_ids)

        logging.info('Waiting for instances to terminate ...')
        ec2.get_waiter('instance_terminated').wait(InstanceIds=instance_ids)

    # Find and delete standalone EBS volumes (not attached to any instance)
    volume_filter = tag_filter + [{'Name': 'status', 'Values': ['available']}]
    volumes = ec2.describe_volumes(Filters=volume_filter)['Volumes']

    for vol in volumes:
        volume_id = vol['VolumeId']
        logging.info(f'Deleting volume {volume_id} ...')
        ec2.delete_volume(VolumeId=volume_id)

    total = len(instance_ids) + len(volumes)
    logging.info(f'Terminated {len(instance_ids)} instance(s) and deleted {len(volumes)} volume(s)')
    return total


def extract_data_volumes_from_template(template):
    '''Removes non-root block device mappings from the template and returns them separately.

    Returns a tuple of (modified_template, data_volumes) where data_volumes is a list of
    dicts with 'DeviceName' and 'Ebs' keys extracted from the template.
    '''
    modified = copy.deepcopy(template)
    root_device = '/dev/sda1'
    data_volumes = []

    for bdm in modified.get('BlockDeviceMappings', []):
        if bdm['DeviceName'] != root_device:
            data_volumes.append(bdm)

    modified['BlockDeviceMappings'] = [
        bdm for bdm in modified.get('BlockDeviceMappings', []) if bdm['DeviceName'] == root_device
    ]

    return modified, data_volumes


def create_and_attach_volumes(ec2, data_volumes, instances, clustertag, source_volume_id=None):
    '''Creates (or copies) data volumes and attaches them to the given instances.

    For each entry in data_volumes and each instance, either creates a new volume from the
    EBS configuration in the template, or copies from source_volume_id if provided.
    '''

    volume_tags = [{
        'Key': 'owner',
        'Value': 'kaloian.manassiev'
    }, {
        'Key': 'mongo_ctools_cluster',
        'Value': clustertag
    }]

    volume_attachments = []

    for vol_def in data_volumes:
        device_name = vol_def['DeviceName']
        ebs_config = vol_def['Ebs']

        created_volume_ids = []
        for instance in instances:
            az = instance['Placement']['AvailabilityZone']
            instance_id = instance['InstanceId']

            if source_volume_id:
                tags = volume_tags + [{'Key': 'source_volume', 'Value': source_volume_id}]
                logging.info(f'Copying volume {source_volume_id} in {az} for {instance_id} ...')
                response = ec2.copy_volumes(
                    SourceVolumeId=source_volume_id, VolumeType=ebs_config.get('VolumeType', 'gp3'),
                    TagSpecifications=[{
                        'ResourceType': 'volume',
                        'Tags': tags
                    }])
                volume_id = response['Volumes'][0]['VolumeId']
            else:
                create_params = {
                    'AvailabilityZone': az,
                    'VolumeType': ebs_config.get('VolumeType', 'gp3'),
                    'Size': ebs_config['VolumeSize'],
                    'Encrypted': ebs_config.get('Encrypted', True),
                    'TagSpecifications': [{
                        'ResourceType': 'volume',
                        'Tags': volume_tags
                    }],
                }
                if 'Iops' in ebs_config:
                    create_params['Iops'] = ebs_config['Iops']
                if 'Throughput' in ebs_config:
                    create_params['Throughput'] = ebs_config['Throughput']

                logging.info(
                    f'Creating {ebs_config["VolumeSize"]}GB {ebs_config.get("VolumeType", "gp3")} '
                    f'volume in {az} for {instance_id} ...')
                volume = ec2.create_volume(**create_params)
                volume_id = volume['VolumeId']

            created_volume_ids.append((volume_id, instance))
            logging.info(f'Volume {volume_id} initiated')

        # Wait for all volumes to become available
        for volume_id, _ in created_volume_ids:
            logging.info(f'Waiting for volume {volume_id} to become available ...')
            ec2.get_waiter('volume_available').wait(VolumeIds=[volume_id], WaiterConfig={
                'Delay': 15,
                'MaxAttempts': 120
            })

        # Attach volumes to instances
        for volume_id, instance in created_volume_ids:
            instance_id = instance['InstanceId']
            logging.info(f'Attaching volume {volume_id} to {instance_id} as {device_name} ...')
            ec2.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device=device_name)
            volume_attachments.append((volume_id, instance_id, device_name))

    # Wait for all attachments and set DeleteOnTermination
    for volume_id, instance_id, device_name in volume_attachments:
        ec2.get_waiter('volume_in_use').wait(VolumeIds=[volume_id])
        ec2.modify_instance_attribute(
            InstanceId=instance_id, BlockDeviceMappings=[{
                'DeviceName': device_name,
                'Ebs': {
                    'DeleteOnTermination': True
                }
            }])

    logging.info(f'All {len(volume_attachments)} volume(s) created and attached successfully')
