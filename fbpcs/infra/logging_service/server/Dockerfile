FROM python:3.9-slim

ARG SERVER_FOLDER=/logging_service/fbpcs/infra/logging_service/server
ENV SERVER_FOLDER_ENV=${SERVER_FOLDER}
ENV COMMAND_LINE_ENV="python ${SERVER_FOLDER_ENV}/server.py"

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get update && apt-get install -y \
    gcc=4:10.2.1-1 \
    vim

RUN pip3 install \
    cython==0.29.30 \
    docopt>=0.6.2 \
    schema==0.7.0 \
    thriftpy2==0.4.14

RUN mkdir -p ${SERVER_FOLDER}
COPY __init__.py ${SERVER_FOLDER}/../
COPY __init__.py ${SERVER_FOLDER}/../../
COPY __init__.py ${SERVER_FOLDER}/../../../

COPY *.py ${SERVER_FOLDER}/
COPY common/ ${SERVER_FOLDER}/common/
COPY thrift/ ${SERVER_FOLDER}/thrift/

EXPOSE 9090
ENTRYPOINT echo Running: ${COMMAND_LINE_ENV} ; export PYTHONPATH=/logging_service ; ${COMMAND_LINE_ENV}
