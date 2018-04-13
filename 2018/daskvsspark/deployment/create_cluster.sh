#!/usr/bin/env bash

aws emr create-cluster \
--applications Name=Hadoop Name=Spark Name=Ganglia Name=Zeppelin \
--bootstrap-actions '[{"Path":"s3://parsely-public/jbennet/daskvsspark/reqs/bootstrap.sh","Name":"Dask Bootstrap"}]' \
--ebs-root-volume-size 20 \
--ec2-attributes '{"KeyName":"emr_jobs","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-ca9b41bd","EmrManagedSlaveSecurityGroup":"sg-f6a19e93","EmrManagedMasterSecurityGroup":"sg-f7a19e92"}' \
--service-role EMR_DefaultRole \
--release-label emr-5.11.1 \
--log-uri 's3n://parsely-emr-logs/' \
--name 'IT Test Cluster' \
--configurations file://./conf.json \
--instance-groups file://./instances.json \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-east-1
