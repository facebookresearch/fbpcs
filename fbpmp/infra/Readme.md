This guide will show you how to setup the AWS infrastructure into your AWS account.

# Prerequisites
* One AWS Account.
* Install Terraform.
* Install AWS CLI and config AWS.
* Terraform Scripts under `./aws_terraform_template`.
* One S3 buckets to store the Terraform state, e.g. `aws-infra-tfstate-partner`.

# AWS Infrastructure Deployment
1. Deploy Terraform scripts under `./aws_terraform_template/networking/` folder.
  * Go into the folder.
  * Run following command to init the Terraform script.

        terraform init \
            -backend-config "bucket=aws-infra-tfstate-partner" \
            -backend-config "region=us-west-2" \
            -backend-config "key=networking.tfstate"

  * Run following command to apply the Terraform script.

        terraform apply \
            -var "aws_region=us-west-2" \
            -var "tag_postfix=-partner" \
            -var "vpc_cidr=10.0.0.0/16" \
            -var "subnet0_cidr=10.0.0.0/17" \
            -var "subnet1_cidr=10.0.128.0/17" \
            -var "publisher_vpc_cidrs=[\"10.1.0.0/16\"]"

  * Take notes of the output from the above command, including following values:

        subnet0_id
        subnet1_id
        pl_efs_security_group_id
        route_table_id

2. Deploy Terraform scripts under `./aws_terraform_template/ecs` folder.
  * Go into the folder.
  * Run following command to init the Terraform script.

        terraform init \
            -backend-config "bucket=aws-infra-tfstate-partner" \
            -backend-config "region=us-west-2" \
            -backend-config "key=ecs.tfstate"

  * Run following command to apply the Terraform script. Remember to replace some of the values to the correct ones.

        terraform apply \
            -var "aws_region=us-west-2" \
            -var "tag_postfix=-partner" \
            -var "aws_account_id=<YOUR_AWS_ACCOUNT_ID>" \
            -var "subnet0_id=<SUBNET0_ID>" \
            -var "subnet1_id=<SUBNET1_ID>" \
            -var "pl_efs_security_group_id=<PL_EFS_SECURITY_GROUP_ID>" \
            -var 'data_processing_ecs_container_image=<IMAGE_URI>' \
            -var 'pid_ecs_container_image=<IMAGE_URI>' \
            -var 'pl_ecs_container_image=<IMAGE_URI>'

3. (Optional) Issue VPC peering connection request to the publisher.
  * Go into the `./aws_terraform_template/vpc_peering/` folder.
  * Run following command to init the Terraform script.

        terraform init \
            -backend-config "bucket=aws-infra-tfstate-partner" \
            -backend-config "region=us-west-2" \
            -backend-config "key=vpcpeering.tfstate"


  * Run following command to apply the Terraform script.

        terraform apply \
            -var "aws_region=us-west-2" \
            -var "tag_postfix=-partner" \
            -var "peer_aws_account_id=<PUBLISHER_AWS_ACCOUNT_ID>" \
            -var "peer_vpc_id=<PUBLISHER_VPC_ID>" \
            -var "vpc_id=<PARTNER_VPC_ID>"

  * Take notes of the output from the above command, including following values:

        vpc_peering_connection_id

4. (Optional) Add one route into the route table.
  * Go into the `./aws_terraform_template/traffic_route/` folder.
  * Run following command to init the Terraform script.

        terraform init \
            -backend-config "bucket=aws-infra-tfstate-partner" \
            -backend-config "region=us-west-2" \
            -backend-config "key=trafficroute.tfstate"

  * Run following command to apply the Terraform script.

        terraform apply \
            -var "route_table_id=<ROUTE_TABLE_ID>" \
            -var "destination_cidr_block=10.1.0.0/16" \
            -var "vpc_peering_connection_id=<VPC_PEERING_CONNECTION_ID>"

# Notes
Step 1 and Step 2 will deploy the key AWS infrastructure into your account.
Step 3 and Step 4 will only be needed to connect the publisher and partner together.

# Destroy the deployed AWS resources
After you finish testing and you want to destroy all the deployed resources, please follow these steps.
1. Destroy VPC Peering Connection
  * cd into partner/vpc_peering/ folder
  * Run following command to destroy

        terraform destroy \
            -var "aws_region=$region" \
            -var "tag_postfix=$tag_postfix"

2. Destroy ECS related resources
  * cd into partner/ecs/ folder
  * Run following command to destroy

        terraform destroy \
            -auto-approve \
            -var "aws_region=$region" \
            -var "tag_postfix=$tag_postfix" \
            -var "aws_account_id=$aws_account_id"

3. Destroy networking related resources
  * cd into partner/networking/ folder
  * Run following command to destroy

        terraform destroy \
            -auto-approve \
            -var "aws_region=$region" \
            -var "tag_postfix=$tag_postfix"
