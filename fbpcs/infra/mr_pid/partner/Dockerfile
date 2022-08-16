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
# Copy mrpid_partner_deploy.sh, Terraform scripts, and config.yml template
##########################################
RUN mkdir -p /terraform_deployment/config
COPY mrpid_partner_deploy.sh /terraform_deployment
RUN chmod +x /terraform_deployment/mrpid_partner_deploy.sh
COPY util.sh /terraform_deployment
RUN chmod +x /terraform_deployment/util.sh
COPY partner /terraform_deployment/terraform_scripts
COPY config.yml /terraform_deployment/config
