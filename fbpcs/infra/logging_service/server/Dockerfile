FROM python:3.9-slim

ARG SERVER_FOLDER=/logging_service/server
ENV SERVER_FOLDER_ENV=${SERVER_FOLDER}
ENV COMMAND_LINE_ENV="python ${SERVER_FOLDER_ENV}/server.py"

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y \
    awscli \
    git \
    unzip \
    vim

RUN pip3 install \
    thrift

COPY *.py ${SERVER_FOLDER}/
COPY common/ ${SERVER_FOLDER}/common/
COPY meta/ ${SERVER_FOLDER}/meta/

EXPOSE 9090
ENTRYPOINT echo Running: ${COMMAND_LINE_ENV} & ${COMMAND_LINE_ENV}
