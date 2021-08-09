# Data Processing Docker Images README
This document describes common use cases (building, testing, extracting) for the Data Processing Docker Image

## Building

Note: At this time, FB does not allow docker builds on FB devservers.  In order to build data_processing docker image(s) you will need to either clone
fbcode locally to a docker capable machine (your laptop) or at a minimum copy the "data_processing" folder to a machine that is capable of using docker.

### Prerequisite

Data Processing docker image uses the fbpcf docker image as its base.  We currently do not have a public docker registry, so this will need to be build locally first.
You have two options to build this image...
* Option 1: Clone https://github.com/facebookresearch/fbpcf from github and simply run `build_docker.sh` in the root of the project
* Option 2: (If you cloned fbcode locally) Goto fbsource/fbcode/measurement/private_measurement/oss and simply run "build_docker.sh" in the root of the project

Note: If building locally you may need to increase the docker memory resources to 4GB in the preferences

### `build-docker.sh` (building emp game images)

To build data processing and `data_processing:latest` docker image run the following script
- `./build-docker.sh
  - build-docker currently only supports Ubuntu but we might support Alpine in the future

## Manual Testing

### Attribution ID Combiner

Simply run `docker/run-attribution_id_combiner-test.sh` and output will be stored in `test-output` folder (created automatically)

## Uploading to AWS Docker Registry

To upload to a new data_processing docker image to AWS

Prerequisite: aws cli must be installed
  - MacOS: `brew install awscli`

Upload/update in AWS:
- `aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 539290649537.dkr.ecr.us-west-2.amazonaws.com`
- `docker tag data_processing:latest 539290649537.dkr.ecr.us-west-2.amazonaws.com/data-processing:latest`
- `docker push 539290649537.dkr.ecr.us-west-2.amazonaws.com/data-processing:latest`
