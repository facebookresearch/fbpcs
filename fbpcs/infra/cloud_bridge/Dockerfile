FROM openjdk:8-jdk-alpine

##########################################
# Install packages
##########################################
RUN apk --update --no-cache add \
    curl \
    git \
    jq \
    libc6-compat \
    openssh-client \
    py-pip \
    python \
    python3 \
    vim

##########################################
# Install python modules
##########################################
RUN pip install \
    awscli

##########################################
# Install Terraform
##########################################
ENV TERRAFORM_VERSION 0.14.9

# Download Terraform
RUN cd /usr/local/bin && \
    curl https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -o terraform.zip && \
    unzip terraform.zip && \
    rm terraform.zip

# Check that it's installed
RUN terraform --version

##########################################
# Copy deploy.sh, Terraform scripts, and config.yml template
##########################################
RUN mkdir /terraform_deployment
COPY deploy.sh /terraform_deployment
RUN chmod +x /terraform_deployment/deploy.sh
COPY util.sh /terraform_deployment
RUN chmod +x /terraform_deployment/util.sh
COPY aws_terraform_template /terraform_deployment/terraform_scripts
COPY data_ingestion /terraform_deployment/terraform_scripts/data_ingestion
COPY semi_automated_data_ingestion /terraform_deployment/terraform_scripts/semi_automated_data_ingestion
COPY config.yml /terraform_deployment
COPY data_validation /terraform_deployment/terraform_scripts/data_validation

# #########################################
# Spring Boot
# #########################################
ARG JAR_FILE=server/build/libs/*.jar
COPY ${JAR_FILE} /server/server.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/server/server.jar"]
