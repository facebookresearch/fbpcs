FROM python:3.8-slim

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y \
    awscli \
    gcc=4:10.2.1-1 \
    git \
    unzip \
    vim

RUN mkdir /pl_coordinator_env
COPY fbpcs/ /pl_coordinator_env/fbpcs/
WORKDIR /pl_coordinator_env
RUN python3.8 -m pip install -r fbpcs/pip_requirements.txt

CMD ["/bin/bash"]
