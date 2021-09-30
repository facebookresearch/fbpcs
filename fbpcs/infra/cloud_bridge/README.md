This README guide will show you how to run `deploy.sh`.

# Prerequisites
(Assume `make` is pre-installed)
Install docker and Java
1. Install docker here: https://www.docker.com/products/docker-desktop
2. Install Java, and make sure using Java 11.
  1. go to: https://java.com/en/download/help/download_options.html and following the installation instruction there. Or Use Homebrew. Run the following command:
```
brew install java11 `or` brew cask install java11
```
  2. run the following command :
```
java -version
```
    you should see the similar messages as follow:
```
openjdk version "11.0.12" 2021-07-20
OpenJDK Runtime Environment Homebrew (build 11.0.12+0)
OpenJDK 64-Bit Server VM Homebrew (build 11.0.12+0, mixed mode)
```


# Run `deploy.sh`

1. Download the `infra` directory
  * run the following command:
```
git clone https://github.com/facebookresearch/fbpcs.git
```
2. Change to `cloud_bridge` directory
  * run the following command
```
cd fbpcs/infra/cloud_bridge
```
  * if encountered permission error, run the following:
```
chmod +x server/gradlew
```
4. build the image
  * run the following command
```
make image-build
```
5. find your image tag/id:
  * run one of the following commands
```
docker image ls
```
`or`
```
docker images cloudbridge-private_lift-server
```
6. given the right docker image tag/id, do `docker run`
  * run the following command
```
docker run -it --entrypoint=/bin/sh <image-name:image-tag>
```
7. show `deploy.sh` arguments and usage
  * run the following command
```
/bin/sh ./terraform_deployment/deploy.sh --help
```
8. create environment variables on AWS credentials (while be removed eventually)
  * run the following command
```
export AWS_ACCESS_KEY_ID=<YOUR_OWN_AWS_ACCESS_KEY> \
export AWS_SECRET_ACCESS_KEY=<YOUR_OWN_AWS_SECRET_ACCESS_KEY> \
export TF_LOG=DEBUG \
export TF_LOG_PATH=/tmp/deploy.log
```
9. run `deploy.sh`
 * For standard `deploy`, run the following command
```
/bin/sh ./terraform_deploment/deploy.sh -r <> -a <> -p <> -v <> -s <> -d <> -t <>
```

 * For to deploy with semi-automated data ingestion, run the following command
```
/bin/sh ./terraform_deploment/deploy.sh -r <> -a <> -p <> -v <> -s <> -d <> -t <> -b
```

 * To undeploy PCE and full/standard data ingestion, run the following command
```
/bin/sh ./terraform_deploment/deploy.sh -r <> -a <> -p <> -v <> -s <> -d <> -t <> -u
```

* To undeploy PCE and full/standard + manual data ingestion , run the following command
```
/bin/sh ./terraform_deploment/deploy.sh -r <> -a <> -p <> -v <> -s <> -d <> -t <> -b -u
```

# Notes
parameter tag (`-t`) cannot be too long. AWS function/variable name must have length less than or equal to 64.
