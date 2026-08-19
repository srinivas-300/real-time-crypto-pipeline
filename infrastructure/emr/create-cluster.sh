#!/usr/bin/env bash
# Creates the crypto-pipeline-emr cluster: 1 primary + 1 core, m5.xlarge (EMR's
# minimum supported instance size - .large sizes don't exist for EMR in any
# family), Spark only, EMR 7.13.0, private subnet.
#
# NOTE: a custom security group in a private subnet requires an additional
# ServiceAccessSecurityGroup (the channel the EMR control plane uses to reach
# the cluster) - without it, RunJobFlow fails validation immediately. See
# docs/BUILD_LOG.md Phase 7 for how this was discovered.
set -euo pipefail

AWS="${AWS_CLI:-aws}"
VPC_ID="vpc-068595398cfb311a5"
PRIVATE_SUBNET_A="subnet-0bbc264afd12cb2ce"
SG_EMR="sg-074e69b04a381d87e"

# Idempotent: the pause/resume cycle deletes the EMR cluster but never this SG
# (it's not part of the pause teardown), so a second run of this script must not
# blindly try to recreate it - create-security-group and authorize-security-group-*
# are NOT idempotent and error out on a second run otherwise (hit this for real on
# the first pause/resume cycle after Phase 13 planning began).
SG_SVC=$("$AWS" ec2 describe-security-groups \
  --filters "Name=group-name,Values=crypto-pipeline-emr-service-access-sg" "Name=vpc-id,Values=$VPC_ID" \
  --query "SecurityGroups[0].GroupId" --output text 2>/dev/null || true)

if [ -z "$SG_SVC" ] || [ "$SG_SVC" = "None" ]; then
  SG_SVC=$("$AWS" ec2 create-security-group --group-name crypto-pipeline-emr-service-access-sg \
    --description "EMR service access channel for private-subnet cluster" --vpc-id "$VPC_ID" \
    --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=crypto-pipeline-emr-service-access-sg}]' \
    --query "GroupId" --output text)

  # Bidirectional: both SGs need both an ingress and egress rule on 9443 for the
  # other - EMR validates this explicitly and rejects cluster creation otherwise.
  "$AWS" ec2 authorize-security-group-egress --group-id "$SG_SVC" \
    --ip-permissions "IpProtocol=tcp,FromPort=9443,ToPort=9443,UserIdGroupPairs=[{GroupId=$SG_EMR,Description='HTTPS to EMR master'}]"
  "$AWS" ec2 authorize-security-group-ingress --group-id "$SG_EMR" \
    --ip-permissions "IpProtocol=tcp,FromPort=9443,ToPort=9443,UserIdGroupPairs=[{GroupId=$SG_SVC,Description='EMR service access HTTPS'}]"
  "$AWS" ec2 authorize-security-group-ingress --group-id "$SG_SVC" \
    --ip-permissions "IpProtocol=tcp,FromPort=9443,ToPort=9443,UserIdGroupPairs=[{GroupId=$SG_EMR,Description='EMR master to service access'}]"
else
  echo "Reusing existing service-access SG: $SG_SVC"
fi

# NOTE ON SERVICE ROLE POLICY: AmazonEMRServicePolicy_v2 (the newer, tag-gated
# managed policy) was tried first and rejected cluster creation repeatedly even
# after tagging the subnet and security groups correctly (its
# CreateEMRTaggedInstancesAndVolumes statement requires the *RunInstances
# request itself* to carry a specific request tag - something that should be
# automatic on EMR's side but wasn't behaving as documented here). Switched to
# the classic AmazonElasticMapReduceRole managed policy instead - broader
# permissions, no tag conditions, no custom-role-name PassRole restriction.
# Still an AWS-authored managed policy, not a custom one. See docs/BUILD_LOG.md
# Phase 7 for the full troubleshooting trail.
# attach-role-policy is itself idempotent (a no-op success if already attached).
"$AWS" iam attach-role-policy \
  --role-name crypto-pipeline-emr-service-role \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

CLUSTER_ID=$("$AWS" emr create-cluster \
  --name crypto-pipeline-emr \
  --release-label emr-7.13.0 \
  --applications Name=Spark \
  --service-role crypto-pipeline-emr-service-role \
  --ec2-attributes "InstanceProfile=crypto-pipeline-emr-ec2-profile,SubnetId=$PRIVATE_SUBNET_A,EmrManagedMasterSecurityGroup=$SG_EMR,EmrManagedSlaveSecurityGroup=$SG_EMR,ServiceAccessSecurityGroup=$SG_SVC" \
  --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,InstanceCount=1,InstanceType=m5.xlarge \
  --log-uri s3://crypto-pipeline-388381628350/emr-logs/ \
  --tags Project=crypto-pipeline \
  --query "ClusterId" --output text)

echo "CLUSTER_ID=$CLUSTER_ID"
echo "Poll status: aws emr describe-cluster --cluster-id $CLUSTER_ID --query Cluster.Status.State"
echo "Terminate when done: aws emr terminate-clusters --cluster-ids $CLUSTER_ID"
