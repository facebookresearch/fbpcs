# Build and Release Pipelines

## Overview of the Pipeline architecture
Coming Soon!

## Updating the FBPCF version in FBPCS
As part of an ongoing effort to improve the FBPCS build and release, we are making some changes to how we build the images and publish them to the GitHub container registry. As part of this, for a temporary time, the FBPCF version is contained in 2 different files. You can find it in [build_binary_images.yml](build_binary_images.yml) on line 8 and [docker_publish.yml](docker_publish.yml) on line 29. Please make sure to update it in both locations whenever you are commiting a version change. To find the version of FBPCF that you want to pin, you can look at their [releases page](https://github.com/facebookresearch/fbpcf/releases) and pick either the latest version released, or whatever version you are looking for.
