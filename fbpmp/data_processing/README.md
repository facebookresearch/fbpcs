# Data Processing Docker Images README
This document describes common use cases (building, testing, extracting) for the Data Processing Docker Image

## Building

Note: At this time, FB does not allow docker builds on FB devservers.  In order to build data_processing docker image(s) you will need to either clone
fbcode locally to a docker capable machine (your laptop) or at a minimum copy the "data_processing" folder to a machine that is capable of using docker.

### Prerequisite

EMP Games docker image(s) uses the fbpcf docker image as its base.  The latest will automaticaly be pulled by the build_docker.sh.

### `build-docker.sh data_processing` (building data processing image)

To build data processing and `data_processing:<TAG>` docker image run the following script
- `./build-docker.sh data_processing`
  - build-docker currently only supports Ubuntu but we might support Alpine in the future
Optionally specify:
* `-t` to tag the image with a given tag (default is 'latest')

## Manual Testing

### Attribution ID Combiner

Simply run `docker/data_processing/run-attribution_id_combiner-test.sh` and output will be stored in `test-output` folder (created automatically)

## Extracting Binaries for Production (OneDocker)

To extract the binaries to upload to S3, simply run the `extract-docker-binaries.sh data_processing` script, and the binaries will be placed `binaries_out` folder
