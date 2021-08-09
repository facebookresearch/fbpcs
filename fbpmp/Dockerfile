FROM python:3.8-slim

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y \
    awscli \
    git \
    unzip \
    vim

RUN mkdir /pl_coordinator_env
COPY fbpmp/ /pl_coordinator_env/fbpmp/
WORKDIR /pl_coordinator_env
RUN python3.8 -m pip install -r fbpmp/pip_requirements.txt

CMD ["/bin/bash"]
