#!/usr/bin/env bash
# Creates the crypto-pipeline VPC: 1 public subnet + 2 private subnets (2 AZs),
# Internet Gateway, single NAT Gateway, S3 gateway endpoint, and the 3 security
# groups (producer / MSK / EMR). Region: us-east-1.
#
# Not idempotent — re-running will create duplicate resources. Intended as a
# reproducible record of what was built, and a starting point for a teardown
# script (see docs/BUILD_LOG.md for every resource ID created).
set -euo pipefail

AWS="${AWS_CLI:-aws}"
REGION="us-east-1"

VPC_ID=$("$AWS" ec2 create-vpc --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=Name,Value=crypto-pipeline-vpc}]' \
  --query "Vpc.VpcId" --output text)

"$AWS" ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-support "{\"Value\":true}"
"$AWS" ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames "{\"Value\":true}"

PUBLIC_SUBNET=$("$AWS" ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.1.0/24 --availability-zone "${REGION}a" \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=crypto-pipeline-public-1a}]' \
  --query "Subnet.SubnetId" --output text)
PRIVATE_SUBNET_A=$("$AWS" ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.11.0/24 --availability-zone "${REGION}a" \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=crypto-pipeline-private-1a}]' \
  --query "Subnet.SubnetId" --output text)
PRIVATE_SUBNET_B=$("$AWS" ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block 10.0.12.0/24 --availability-zone "${REGION}b" \
  --tag-specifications 'ResourceType=subnet,Tags=[{Key=Name,Value=crypto-pipeline-private-1b}]' \
  --query "Subnet.SubnetId" --output text)

IGW_ID=$("$AWS" ec2 create-internet-gateway \
  --tag-specifications 'ResourceType=internet-gateway,Tags=[{Key=Name,Value=crypto-pipeline-igw}]' \
  --query "InternetGateway.InternetGatewayId" --output text)
"$AWS" ec2 attach-internet-gateway --vpc-id "$VPC_ID" --internet-gateway-id "$IGW_ID"
"$AWS" ec2 modify-subnet-attribute --subnet-id "$PUBLIC_SUBNET" --map-public-ip-on-launch

PUBLIC_RT=$("$AWS" ec2 create-route-table --vpc-id "$VPC_ID" \
  --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=crypto-pipeline-public-rt}]' \
  --query "RouteTable.RouteTableId" --output text)
"$AWS" ec2 create-route --route-table-id "$PUBLIC_RT" --destination-cidr-block 0.0.0.0/0 --gateway-id "$IGW_ID"
"$AWS" ec2 associate-route-table --route-table-id "$PUBLIC_RT" --subnet-id "$PUBLIC_SUBNET"

PRIVATE_RT=$("$AWS" ec2 create-route-table --vpc-id "$VPC_ID" \
  --tag-specifications 'ResourceType=route-table,Tags=[{Key=Name,Value=crypto-pipeline-private-rt}]' \
  --query "RouteTable.RouteTableId" --output text)
"$AWS" ec2 associate-route-table --route-table-id "$PRIVATE_RT" --subnet-id "$PRIVATE_SUBNET_A"
"$AWS" ec2 associate-route-table --route-table-id "$PRIVATE_RT" --subnet-id "$PRIVATE_SUBNET_B"

EIP_ALLOC=$("$AWS" ec2 allocate-address --domain vpc \
  --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=crypto-pipeline-nat-eip}]' \
  --query "AllocationId" --output text)
NAT_ID=$("$AWS" ec2 create-nat-gateway --subnet-id "$PUBLIC_SUBNET" --allocation-id "$EIP_ALLOC" \
  --tag-specifications 'ResourceType=natgateway,Tags=[{Key=Name,Value=crypto-pipeline-nat}]' \
  --query "NatGateway.NatGatewayId" --output text)
"$AWS" ec2 wait nat-gateway-available --nat-gateway-ids "$NAT_ID"
"$AWS" ec2 create-route --route-table-id "$PRIVATE_RT" --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT_ID"

"$AWS" ec2 create-vpc-endpoint --vpc-id "$VPC_ID" --service-name "com.amazonaws.${REGION}.s3" \
  --route-table-ids "$PUBLIC_RT" "$PRIVATE_RT" \
  --tag-specifications 'ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=crypto-pipeline-s3-endpoint}]'

SG_PRODUCER=$("$AWS" ec2 create-security-group --group-name crypto-pipeline-producer-sg \
  --description "EC2 Kafka producer - outbound only, no inbound" --vpc-id "$VPC_ID" \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=crypto-pipeline-producer-sg}]' \
  --query "GroupId" --output text)
SG_MSK=$("$AWS" ec2 create-security-group --group-name crypto-pipeline-msk-sg \
  --description "MSK brokers - Kafka IAM auth port only from producer and EMR" --vpc-id "$VPC_ID" \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=crypto-pipeline-msk-sg}]' \
  --query "GroupId" --output text)
SG_EMR=$("$AWS" ec2 create-security-group --group-name crypto-pipeline-emr-sg \
  --description "EMR cluster nodes - internal cluster traffic only" --vpc-id "$VPC_ID" \
  --tag-specifications 'ResourceType=security-group,Tags=[{Key=Name,Value=crypto-pipeline-emr-sg}]' \
  --query "GroupId" --output text)

"$AWS" ec2 authorize-security-group-ingress --group-id "$SG_MSK" \
  --ip-permissions "IpProtocol=tcp,FromPort=9098,ToPort=9098,UserIdGroupPairs=[{GroupId=$SG_PRODUCER,Description='Producer IAM-auth Kafka access'}]"
"$AWS" ec2 authorize-security-group-ingress --group-id "$SG_MSK" \
  --ip-permissions "IpProtocol=tcp,FromPort=9098,ToPort=9098,UserIdGroupPairs=[{GroupId=$SG_EMR,Description='EMR IAM-auth Kafka access'}]"
"$AWS" ec2 authorize-security-group-ingress --group-id "$SG_EMR" \
  --ip-permissions "IpProtocol=-1,UserIdGroupPairs=[{GroupId=$SG_EMR,Description='EMR internal cluster traffic'}]"

echo "VPC_ID=$VPC_ID"
echo "PUBLIC_SUBNET=$PUBLIC_SUBNET"
echo "PRIVATE_SUBNET_A=$PRIVATE_SUBNET_A"
echo "PRIVATE_SUBNET_B=$PRIVATE_SUBNET_B"
echo "IGW_ID=$IGW_ID"
echo "NAT_ID=$NAT_ID"
echo "SG_PRODUCER=$SG_PRODUCER"
echo "SG_MSK=$SG_MSK"
echo "SG_EMR=$SG_EMR"
