# EMP Games Docker Images README
This document describes common use cases (building, testing, extracting) for the EMP Games Docker Image(s)

## Building

### Prerequisite

EMP Games docker image(s) uses the fbpcf docker image as its base.  The latest will automatically be pulled by the build_docker.sh.

### Building emp-game Image (`build-docker.sh` Script)

```
Usage: build-docker.sh <package: emp_games|data_processing|onedocker> [-u] [-t <tag>]

package:
  emp_games - builds the emp-games docker image
-u: builds the docker images against ubuntu (default)
-f: force use of latest fbpcf from ghcr.io/facebookresearch
-t <tag>: tags the image with the given tag (default: latest)
```

To build the emp games docker image as `emp-game:<tag>`, run `./build-docker.sh emp_games -t <tag>`.

If `<tag>` is ommited, then `latest` will be used as default.

`build-docker.sh` currently only supports Ubuntu but we might support Alpine in the future.

## Manual Testing

Manual testing scripts have been provided for the lift and attribution calculators and shared shard aggregator.  Output is placed in `sample-output`

### Lift
To run a sample attribution and shard against your newly built attribution docker image, the two scripts are available:
* `docker/emp_games/run-lift-calculator-sample.sh`
* `docker/emp_games/run-shard-aggregator-sample.sh`

### Attribution
To run a sample attribution and shard against your newly built attribution docker image, the two scripts are available:
* `docker/emp_games/run-attribution-sample.sh`
* `docker/emp_games/run-shard-aggregator-sample.sh`

## Extracting Binaries for Production (OneDocker)

To extract the binaries to upload to S3, simply run the `extract-docker-binaries.sh emp_games` script, and the binaries will be placed `binaries_out` folder
