# EMP Games Docker Images README
This document describes common use cases (building, testing, extracting) for the EMP Games Docker Image(s)

## Building

Note: At this time, FB does not allow docker builds on FB devservers.  In order to build emp_games docker image(s) you will need to either clone
fbcode locally to a docker capable machine (your laptop) or at a minimum copy the "emp_games" folder to a machine to a capable of using docker.

### Prerequisite

EMP Games docker image(s) uses the fbpcf docker image as its base.  We currently do not have a public docker registry, so this will need to be build locally first.
* Clone https://github.com/facebookresearch/fbpcf from github and simply run `build_docker.sh` in the root of the project

Note: If building locally you may need to increase the docker memory resources to 4GB in the preferences

### `build-docker.sh` (building emp game images)

To build the emp games and `emp_game:<TAG>` docker image run `./build-docker.sh`. build-docker currently only supports Ubuntu but we might support Alpine in the future

Optionally specify:
* `-t` to tag the image with a given tag (default is 'latest')


## Manual Testing

Manual testing scripts have been provided for the lift and attribution calculators and shared shard aggregator.  Output is placed in `sample-output`

### Lift
To run a sample attribution and shard against your newly built attribution docker image, the two scripts are available:
* `docker/emp_game/run-lift-calculator-sample.sh`
* `docker/emp_game/run-shard-aggregator-sample.sh`


### Attribution
To run a sample attribution and shard against your newly built attribution docker image, the two scripts are available:
* `docker/emp_game/run-attribution-sample.sh`
* `docker/emp_game/run-shard-aggregator-sample.sh`

## Extracting Binaries for Production (OneDocker)

To extract the binaries to upload to S3, simply run the `extract-docker-binaries.sh` script, and the binaries will be placed `binaries_out` folder
