FROM python:3.8-slim

# Install Dependencies
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y \
    awscli \
    gcc=4:10.2.1-1 \
    git \
    unzip \
    vim

# Create the pcs user
RUN useradd -ms /bin/bash pcs

# Switch to pcs user
USER pcs

RUN mkdir /home/pcs/pl_coordinator_env
COPY fbpcs/ /home/pcs/pl_coordinator_env/fbpcs/
WORKDIR /home/pcs/pl_coordinator_env

# Switch back to root to ensure root user run compatible
USER root
RUN python3.8 -m pip install -r fbpcs/pip_requirements.txt

CMD ["/bin/bash"]
