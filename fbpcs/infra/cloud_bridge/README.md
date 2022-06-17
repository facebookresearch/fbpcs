This README guide will show you how to run `deploy.sh`, for the partner-side deployment.

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
  * run the following command to open the docker image in an interactive shell
```
docker run -it --entrypoint=/bin/bash <image-name:image-tag>  (without the '<' and '>')
example:  docker run -it --entrypoint=/bin/bash cloudbridge-private_lift-server:0.0.1
```
7. show `deploy.sh` arguments and usage
  * run the following command
```
cd /terraform_deployment
/bin/bash deploy.sh --help
```
8. create environment variables on AWS credentials (while be removed eventually)
  * run the following command
```
Note: Make sure you have the credentials `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to access the AWS account.
The `AWS_SESSION_TOKEN` is optional if you have a permanent pair of `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.

export AWS_ACCESS_KEY_ID=<YOUR_OWN_AWS_ACCESS_KEY> \
export AWS_SECRET_ACCESS_KEY=<YOUR_OWN_AWS_SECRET_ACCESS_KEY> \
export AWS_SESSION_TOKEN=<YOUR_OWN_AWS_SESSION_TOKEN> \
export TF_LOG=DEBUG \
export TF_LOG_PATH=/tmp/deploy.log \
export TF_LOG_STREAMING=/tmp/deploymentStream.log
```
9. run `deploy.sh`
 * For standard `deploy` and without any semi-automated data ingestion, run the following command

```
/bin/bash deploy.sh deploy -r <> -t <> -a <> -p <> -v <> -s<optional> -d<optional> -b<optional>
example: "/bin/bash deploy.sh deploy -r us-west-2 -t "your-tag-name" -a 627672676272 -p 43454354533545 -v vpc-036652587a2d1839c
```

 * For standard `undeploy` and without any semi-automated data ingestion, run the following command
```
/bin/bash deploy.sh undeploy -r <> -t <> -a <> -p <> -v <> -s<optional> -d<optional> -b<optional>
example: "/bin/bash deploy.sh undeploy -r us-west-2 -t "your-tag-name" -a 627672676272 -p 43454354533545 -v vpc-036652587a2d1839c

```

 * For deploy with semi-automated data ingestion and without using any bucket names, run the following command
```
/bin/bash deploy.sh deploy -r <> -t <> -a <> -p <> -v <> -b<optional>

example: /bin/bash deploy.sh deploy -r us-west-2 -t "your-tag-name" -a 592513842793 -p 539290649537 -v vpc-036652587a2d1839c -b

```

 * For undeploy with semi-automated data ingestion and without using any bucket names, run the following command
```
/bin/bash deploy.sh undeploy -r <> -t <> -a <> -p <> -v <> -s<optional> -d<optional> -b<optional>

example: /bin/bash deploy.sh undeploy -r us-west-2 -t "your-tag-name" -a 592513842793 -p 539290649537 -v vpc-036652587a2d1839c -b

```

 * For deploy with semi-automated data ingestion and  using manual bucket names, run the following command
```
/bin/bash deploy.sh deploy -r <> -t <> -a <> -p <> -v <> -s<optional> -d<optional> -b<optional>

example: /bin/bash deploy.sh deploy -r us-west-2 -t "your-tag-name" -a 592513842793 -p 539290649537 -v vpc-036652587a2d1839c -b -s storage-bucket-name-optional -d data-storage-bucket-name-optional

```

 * For undeploy with semi-automated data ingestion and without using any bucket names, run the following command
```
/bin/bash deploy.sh undeploy -r <> -t <> -a <> -p <> -v <> -s<optional> -d<optional> -b<optional>

example: /bin/bash deploy.sh undeploy -r us-west-2 -t "your-tag-name" -a 592513842793 -p 539290649537 -v vpc-036652587a2d1839c -b -s storage-bucket-name-optional -d data-storage-bucket-name-optional
```


# Notes
 * parameter tag (`-t`) cannot be too long. AWS function/variable name must have length less than or equal to 64.
 * parameters p, v are the required cloud_account_id and vpc_id values from the existing PCE publisher-side deployment.
 * parameter s, d, b are optional.
