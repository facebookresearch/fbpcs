FROM openjdk:11

##########################################
# Install packages
##########################################
RUN apt-get update && apt-get install -y \
    openssh-client \
    curl \
    git \
    python3 \
    jq \
    vim \
    python3-pip


##########################################
# Install python modules
##########################################
RUN pip install \
    awscli

# Note: the dataclasses backport can be removed once Python 3 is upgraded to 3.7
RUN pip3 install \
    boto3 \
    dataclasses

##########################################
# Install fbpcp modules
##########################################
RUN python3 -m pip install fbpcp

##########################################
# Install Terraform
##########################################
ENV TERRAFORM_VERSION 0.14.9

# Download Terraform, verify checksum, and unzip
WORKDIR /usr/local/bin
RUN curl -SL https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip --output terraform_${TERRAFORM_VERSION}_linux_amd64.zip && \
  curl -SL https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_SHA256SUMS --output terraform_${TERRAFORM_VERSION}_SHA256SUMS && \
  grep terraform_${TERRAFORM_VERSION}_linux_amd64.zip terraform_${TERRAFORM_VERSION}_SHA256SUMS | sha256sum -c - && \
  unzip terraform_${TERRAFORM_VERSION}_linux_amd64.zip && \
  rm terraform_${TERRAFORM_VERSION}_SHA256SUMS && \
  rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip

# Check that it's installed
RUN terraform --version

##########################################
# Copy deploy.sh, Terraform scripts, and config.yml template
##########################################
RUN mkdir -p /terraform_deployment/config
COPY deploy.sh /terraform_deployment
RUN chmod +x /terraform_deployment/deploy.sh
COPY deploy_pc_infra.sh /terraform_deployment
RUN chmod +x /terraform_deployment/deploy_pc_infra.sh
COPY upgrade.sh /terraform_deployment
RUN chmod +x /terraform_deployment/upgrade.sh
COPY util.sh /terraform_deployment
RUN chmod +x /terraform_deployment/util.sh
COPY pceValidator.sh /terraform_deployment
RUN chmod +x /terraform_deployment/pceValidator.sh
COPY aws_terraform_template /terraform_deployment/terraform_scripts
COPY data_ingestion /terraform_deployment/terraform_scripts/data_ingestion
COPY key_injection_agent /terraform_deployment/terraform_scripts/key_injection_agent
COPY clean_up_agent /terraform_deployment/terraform_scripts/clean_up_agent
COPY data_preprocessing /terraform_deployment/terraform_scripts/data_preprocessing
COPY semi_automated_data_ingestion /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
COPY s3_bucket_notification /terraform_deployment/terraform_scripts/s3_bucket_notification
COPY config.yml /terraform_deployment/config
COPY cli.py /terraform_deployment
RUN mkdir -p /terraform_deployment/fbpcs/infra/cloud_bridge
COPY deployment_helper /terraform_deployment/fbpcs/infra/cloud_bridge/deployment_helper
COPY advertiser_infra_logging /terraform_deployment/terraform_scripts/advertiser_infra_logging

###################################################################
# Install cryptography and prerequisite libraries. Needed for KIA.
##################################################################

RUN pip3 install \
    --platform manylinux2010_x86_64 \
    --implementation cp \
    --python 3.9 \
    --only-binary=:all: --upgrade \
    --target awsbundle \
    cryptography -t /terraform_deployment/terraform_scripts/key_injection_agent/kia_source_code/

RUN pip3 install pyqldb -t /terraform_deployment/terraform_scripts/key_injection_agent/kia_source_code/
RUN pip3 install pyion2json -t /terraform_deployment/terraform_scripts/key_injection_agent/kia_source_code/
RUN pip3 install dataclasses-json -t /terraform_deployment/terraform_scripts/key_injection_agent/kia_source_code/
RUN pip3 install injector -t /terraform_deployment/terraform_scripts/key_injection_agent/kia_source_code/
# #########################################
# Spring Boot
# #########################################
ARG JAR_FILE_PATH
COPY ${JAR_FILE_PATH}  /server/server.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/server/server.jar"]
