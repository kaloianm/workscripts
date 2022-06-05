#!/bin/bash
set -e

export SCRIPT_DIR="$(dirname "$0")"
export MONGOVERSION=6.0-EXT4

aws ec2 run-instances \
    --count 3 \
    --launch-template LaunchTemplateId=lt-042b07169886af208 \
    --block-device-mappings "DeviceName=/dev/sdf,Ebs={VolumeType=gp3,VolumeSize=256}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=noreap,Value=true},{Key=mongoversion,Value=$MONGOVERSION},{Key=mongorole,Value=config}]" \
    --user-data file://$SCRIPT_DIR/configure_cluster_host.sh \
    >launch.log 2>&1

aws ec2 run-instances \
    --count 3 \
    --launch-template LaunchTemplateId=lt-042b07169886af208 \
    --block-device-mappings "DeviceName=/dev/sdf,Ebs={VolumeType=gp3,VolumeSize=1500,Iops=3000}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=noreap,Value=true},{Key=mongoversion,Value=$MONGOVERSION},{Key=mongorole,Value=shard0}]" \
    --user-data file://$SCRIPT_DIR/configure_cluster_host.sh \
    >>launch.log 2>&1

aws ec2 run-instances \
    --count 3 \
    --launch-template LaunchTemplateId=lt-042b07169886af208 \
    --block-device-mappings "DeviceName=/dev/sdf,Ebs={VolumeType=gp3,VolumeSize=1500,Iops=3000}" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=noreap,Value=true},{Key=mongoversion,Value=$MONGOVERSION},{Key=mongorole,Value=shard1}]" \
    --user-data file://$SCRIPT_DIR/configure_cluster_host.sh \
    >>launch.log 2>&1

aws ec2 run-instances \
    --count 1 \
    --launch-template LaunchTemplateId=lt-042b07169886af208 \
    --tag-specifications "ResourceType=instance,Tags=[{Key=noreap,Value=true},{Key=mongoversion,Value=$MONGOVERSION},{Key=mongorole,Value=driver}]" \
    --user-data file://$SCRIPT_DIR/configure_perf_client_host.sh \
    >>launch.log 2>&1

aws ec2 describe-instances \
    --filters Name=tag:owner,Values=kaloian.manassiev \
    Name=tag:mongoversion,Values=$MONGOVERSION \
    Name=tag:mongorole,Values=config \
    --query "Reservations[].Instances[].PublicDnsName[]"

aws ec2 describe-instances \
    --filters Name=tag:owner,Values=kaloian.manassiev \
    Name=tag:mongoversion,Values=$MONGOVERSION \
    Name=tag:mongorole,Values=shard0 \
    --query "Reservations[].Instances[].PublicDnsName[]"

aws ec2 describe-instances \
    --filters Name=tag:owner,Values=kaloian.manassiev \
    Name=tag:mongoversion,Values=$MONGOVERSION \
    Name=tag:mongorole,Values=shard1 \
    --query "Reservations[].Instances[].PublicDnsName[]"

aws ec2 describe-instances \
    --filters Name=tag:owner,Values=kaloian.manassiev \
    Name=tag:mongorole,Values=driver \
    --query 'Reservations[].Instances[].{Instance:PublicDnsName, Name:Tags[?Key==`mongoversion`]}'
