'''Common EC2 instance launching and management utilities.'''

import json
import logging
import os

#
# Host configuration scripts. Their output will show up in /var/log/cloud-init-output.log.
#

ANY_HOST_CONFIGURATION = '''#!/bin/bash
set -e

# Detect the default non-root user (ec2-user on Amazon Linux, ubuntu on Ubuntu, etc.)
DEFAULT_USER=$(getent passwd ec2-user 2>/dev/null | cut -d: -f1 || \
               getent passwd ubuntu 2>/dev/null | cut -d: -f1 || \
               echo "nobody")
DEFAULT_USER_HOME=$(getent passwd "$DEFAULT_USER" | cut -d: -f6)

###################################################################################################
echo "Applying OS configuration for user $DEFAULT_USER (Home: $DEFAULT_USER_HOME)"
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
'''

CLIENT_HOST_TEMPLATE = os.path.join(os.path.dirname(__file__), '..', 'ClientHost.json')


def make_client_driver_host_configuration(clustertag):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring driver host workscripts for {clustertag}"
###################################################################################################
'''


def make_cluster_host_configuration(clustertag, filesystem):
    return ANY_HOST_CONFIGURATION + f'''
###################################################################################################
echo "Configuring shard host volumes for {clustertag}"
###################################################################################################

echo "Waiting for volume to be attached ..."
while [ ! -e "/dev/xvdb" ]; do sleep 1; done

if [ ! -e "/dev/xvdb1" ]; then
  echo "Partitioning data volume ..."
  sudo parted -s /dev/xvdb mklabel gpt &&
    sudo parted -s -a optimal /dev/xvdb mkpart primary 0% 100%

  echo "Making {filesystem} filesystem ..."
  while [ ! -e "/dev/xvdb1" ]; do sleep 1; done
  sudo mkfs -t {filesystem} /dev/xvdb1
fi

echo "Mounting data volume ..."
sudo mkdir /mnt/data
sudo mount /dev/xvdb1 /mnt/data
sudo chown -R $DEFAULT_USER:$DEFAULT_USER /mnt/data

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
