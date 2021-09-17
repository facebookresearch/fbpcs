# Data Processing Docker Images README
This document describes common use cases (building, testing, extracting) for the Data Processing Docker Image

### Prerequisite

EMP Games docker image(s) uses the fbpcf docker image as its base.  The latest will automatically be pulled by the build_docker.sh.

### Building data-procesing Image (`build-docker.sh` Script)

```
Usage: build-docker.sh <package: emp_games|data_processing|onedocker> [-u] [-t <tag>]

package:
  data_processing - builds the data_processing docker image
-u: builds the docker images against ubuntu (default)
-f: force use of latest fbpcf from ghcr.io/facebookresearch
-t <tag>: tags the image with the given tag (default: latest)
```

To build the emp games docker image as `data-processing:<tag>`, run `./build-docker.sh data_processing -t <tag>`.

If `<tag>` is ommited, then `latest` will be used as default.

`build-docker.sh` currently only supports Ubuntu but we might support Alpine in the future.

## Manual Testing

### Attribution ID Combiner

Simply run `docker/data_processing/run-attribution_id_combiner-test.sh` and output will be stored in `test-output` folder (created automatically)

## Extracting Binaries for Production (OneDocker)

To extract the binaries to upload to S3, simply run the `extract-docker-binaries.sh data_processing` script, and the binaries will be placed `binaries_out` folder
