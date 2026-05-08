'''
Common EC2 instance launching and management utilities.
'''

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
echo "Generating aliases and scripts"
###################################################################################################

sudo -u ubuntu -i bash -c 'echo "less /var/log/cloud-init-output.log" >> $HOME/.bash_history'
sudo -u ubuntu -i bash -c 'echo "source python3-venv/bin/activate" >> $HOME/.bash_history'
sudo -u ubuntu -i bash -c 'echo "iostat -s 1 --human nvme1n1" >> $HOME/.bash_history'
sudo -u ubuntu -i bash -c 'echo "dstat -cdnmgy" >> $HOME/.bash_history'

###################################################################################################
echo "Configuring required packages ..."
###################################################################################################

sudo apt update -y
sudo apt install -y vim build-essential dstat sysstat lvm2

###################################################################################################
echo "Installing Homebrew ..."
###################################################################################################

sudo -u ubuntu -i bash -c 'curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh | NONINTERACTIVE=1 bash'

sudo -u ubuntu -i bash -c 'echo >> $HOME/.bashrc'
sudo -u ubuntu -i bash -c 'echo '"'"'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"'"'"' >> $HOME/.bashrc'

sudo -u ubuntu -i bash -c 'echo >> $HOME/.bash_profile'
sudo -u ubuntu -i bash -c 'echo '"'"'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"'"'"' >> $HOME/.bash_profile'

eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv bash)"

sudo -u ubuntu -i brew update
sudo -u ubuntu -i brew install python3 mongosh htop

###################################################################################################
echo "Cloning required repositories and tools ..."
###################################################################################################

sudo -u ubuntu -i bash -c 'git clone --single-branch https://github.com/kaloianm/workscripts.git $HOME/workscripts'

sudo -u ubuntu -i python3 -m venv workscripts/python3-venv
sudo -u ubuntu -i workscripts/python3-venv/bin/python3 -m pip install -r workscripts/ctools/requirements.txt

curl -fsSL https://github.com/feliixx/mgodatagen/releases/download/v0.12.0/mgodatagen_0.12.0_linux_arm64.tar.gz | sudo tar -xz -C /usr/local/bin

###################################################################################################
echo "Done!"
###################################################################################################
'''

CLIENT_HOST_TEMPLATE = os.path.join(os.path.dirname(__file__), '..', 'ClientHost.json')


def make_client_driver_host_configuration(clustertag):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring driver host workscripts for {clustertag}"
###################################################################################################
'''


def make_cluster_host_configuration(clustertag, filesystem, skip_format=False):
    data_device = '/dev/datavg/data'
    if skip_format:
        setup_script = f'''
echo "Activating existing LVM volume group on /dev/nvme1n1 ..."
sudo vgchange -ay datavg
while [ ! -e "{data_device}" ]; do sleep 1; done
'''
    else:
        # Thin pool consumes 95% of the VG, leaving headroom for LVM metadata growth. The thin LV's
        # virtual size equals the pool's data size: snapshot copy-on-write writes share this same
        # pool, so the underlying EBS volume must be sized to fit both the dataset and the
        # divergence between origin and snapshot during experiments.
        setup_script = f'''
echo "Setting up LVM thin pool on /dev/nvme1n1 ..."
sudo pvcreate /dev/nvme1n1
sudo vgcreate datavg /dev/nvme1n1
sudo lvcreate --type thin-pool --chunksize 128K -l 95%FREE -n datapool datavg
POOL_SIZE_BYTES=$(sudo lvs --noheadings --nosuffix --units b -o lv_size datavg/datapool | tr -d ' ')
echo "Creating thin volume of ${{POOL_SIZE_BYTES}} bytes ..."
sudo lvcreate -V "${{POOL_SIZE_BYTES}}B" --thin -n data datavg/datapool

echo "Making {filesystem} filesystem on {data_device} ..."
sudo mkfs -t {filesystem} {data_device}
'''

    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring shard host volumes for {clustertag}"
###################################################################################################

echo "Waiting for volume to be attached ..."
while [ ! -e "/dev/nvme1n1" ]; do sleep 1; done
{setup_script}
echo "Mounting data volume ..."
sudo mkdir -p /mnt/data
sudo mount {data_device} /mnt/data
sudo chmod 1777 /mnt/data

echo "Data volume mounted, persisting mount point so it survives reboots ..."
if [ -z $(grep "/mnt/data" "/etc/fstab") ]; then echo $(cat "/proc/mounts" | grep "/mnt/data") >> /etc/fstab; fi

# Snapshot the clean state after loading data:
#   sudo lvcreate -s -kn --name data_clean datavg/data
# Roll back to the clean state (near-instant):
#   sudo umount /mnt/data
#   sudo lvremove -f datavg/data
#   sudo lvcreate -s -kn --name data datavg/data_clean
#   sudo mount /dev/datavg/data /mnt/data
# Drop the snapshot and revert to the clean state as the primary volume:
#   sudo umount /mnt/data
#   sudo lvremove -f datavg/data
#   sudo lvrename datavg/data_clean datavg/data
#   sudo mount /dev/datavg/data /mnt/data
# Monitor pool fill with: sudo lvs -o+data_percent,metadata_percent datavg/datapool

echo "Completed configuration for {clustertag} !"
'''


def make_instance_tag_specifications(clustertag, role, user):
    '''
    Instantiates an AWS instance tag specification for the specified cluster node
    '''
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
        }, {
            'Key': 'owner',
            'Value': user
        }]
    }]


def resolve_security_group_id(ec2, name):
    '''
    Resolves a security group name to its ID
    '''
    response = ec2.describe_security_groups(Filters=[{'Name': 'group-name', 'Values': [name]}])
    groups = response['SecurityGroups']
    if not groups:
        raise ValueError(f'No security group found with name: {name}')
    return groups[0]['GroupId']


def load_template(template_path):
    '''
    Loads a JSON template file and returns its contents as kwargs for run_instances
    '''
    with open(template_path) as f:
        template = json.load(f)

    # MinCount/MaxCount are controlled by the caller, not the template
    template.pop('MinCount', None)
    template.pop('MaxCount', None)

    return template


def launch_instances(ec2, template, tag_specs, user_data, count):
    '''
    Launches EC2 instances using the template as base parameters
    '''

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
    '''
    Waits for the given instances to reach the running state
    '''
    instance_ids = [i['InstanceId'] for i in instances]
    logging.info(f'Waiting for {len(instance_ids)} instances to start running ...')
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=instance_ids)


def describe_all_instances(ec2, clustertag, user):
    '''
    Issues a query to describe all running instances with the specified cluster tag
    '''
    filters = [{
        'Name': 'tag:owner',
        'Values': [user]
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
    '''
    Filters a list of instances by their mongorole tag
    '''
    return list(filter(lambda x: {
        'Key': 'mongorole',
        'Value': role,
    } in x['Tags'], instances))


def terminate_cluster_resources(ec2, clustertag, user):
    '''
    Terminates all instances and deletes all EBS volumes tagged with the given cluster tag
    '''
    tag_filter = [{
        'Name': 'tag:mongo_ctools_cluster',
        'Values': [clustertag]
    }, {
        'Name': 'tag:owner',
        'Values': [user]
    }]

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
    '''
    Removes non-root block device mappings from the template and returns them separately.

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


def create_and_attach_volumes(ec2, data_volumes, instances, clustertag, user,
                              source_volume_id=None):
    '''
    Creates (or copies) data volumes and attaches them to the given instances.

    For each entry in data_volumes and each instance, either creates a new volume from the
    EBS configuration in the template, or copies from source_volume_id if provided.
    '''

    volume_tags = [{
        'Key': 'Name',
        'Value': clustertag
    }, {
        'Key': 'owner',
        'Value': user
    }, {
        'Key': 'mongo_ctools_cluster',
        'Value': clustertag
    }]

    volume_attachments = []

    for vol_def in data_volumes:
        device_name = vol_def['DeviceName']

        # Volume specification shared between the create and copy paths. VolumeSize in the
        # BlockDeviceMapping template is called `Size` in create_volume/copy_volumes;
        # DeleteOnTermination is only valid inside BlockDeviceMappings and is applied later via
        # modify_instance_attribute.
        ebs_spec = {k: v for k, v in vol_def['Ebs'].items() if k != 'DeleteOnTermination'}
        ebs_spec['Size'] = ebs_spec.pop('VolumeSize')

        created_volume_ids = []
        for instance in instances:
            az = instance['Placement']['AvailabilityZone']
            instance_id = instance['InstanceId']

            if source_volume_id:
                # copy_volumes inherits encryption from the source and rejects Encrypted/KmsKeyId
                ebs_spec_copy_volumes = {
                    k: v
                    for k, v in ebs_spec.items() if k not in ('Encrypted', 'KmsKeyId')
                }
                logging.info(
                    f'Copying {ebs_spec["Size"]}GB {ebs_spec["VolumeType"]} volume from {source_volume_id} ...'
                )
                response = ec2.copy_volumes(
                    SourceVolumeId=source_volume_id,
                    **ebs_spec_copy_volumes,
                    TagSpecifications=[{
                        'ResourceType': 'volume',
                        'Tags': volume_tags
                    }],
                )
                volume_id = response['Volumes'][0]['VolumeId']
            else:
                logging.info(f'Creating {ebs_spec["Size"]}GB {ebs_spec["VolumeType"]} volume ...')
                volume = ec2.create_volume(
                    AvailabilityZone=az,
                    **ebs_spec,
                    TagSpecifications=[{
                        'ResourceType': 'volume',
                        'Tags': volume_tags
                    }],
                )
                volume_id = volume['VolumeId']

            created_volume_ids.append((volume_id, instance))
            logging.info(f'Volume {volume_id} created')

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
