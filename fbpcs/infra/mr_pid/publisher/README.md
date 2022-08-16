This README guide will show you how to run `mrpid_publisher_initial_deploy.sh` and `mrpid_publisher_final_deploy.sh`, for the MR-PID publisher-side deployment.

# Prerequisites
(Assume `make` is pre-installed)
Install docker
1. Install docker here: https://www.docker.com/products/docker-desktop

# Run `mrpid_publisher_initial_deploy.sh`

1. Download the `infra` directory
  * run the following command:
```
git clone https://github.com/facebookresearch/fbpcs.git
```
2. Change to `mr_pid/publisher` directory
  * run the following command
```
cd fbpcs/infra/mr_pid/publisher
3. build the image
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
docker images mrpid-publisher-side
```
6. given the right docker image tag/id, do `docker run`
  * run the following command to open the docker image in an interactive shell
```
docker run -it --entrypoint=/bin/bash <image-name:image-tag>  (without the '<' and '>')
example: docker run -it --entrypoint=/bin/bash mrpid-publisher-side:0.0.1
```
7. show `mrpid_publisher_initial_deploy.sh` arguments and usage
  * run the following command
```
cd /terraform_deployment
/bin/bash mrpid_publisher_initial_deploy.sh --help
```
Note: This initial script needs to be run before partner side deployment script!
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
9. run `mrpid_publisher_initial_deploy.sh`
 * For standard `deploy`, run the following command

```
/bin/bash mrpid_publisher_initial_deploy.sh deploy -r <> -t <> -a <> -p <> -b <optional>
example: /bin/bash mrpid_publisher_initial_deploy.sh deploy -r us-west-2 -t your-tag-name -a 627672676272 -p 43454354533545
```

 * For standard `undeploy`, run the following command
```
/bin/bash mrpid_publisher_initial_deploy.sh undeploy -r <> -t <> -a <> -p <> -b <optional>
example: /bin/bash mrpid_publisher_initial_deploy.sh undeploy -r us-west-2 -t your-tag-name -a 627672676272 -p 43454354533545
```
10. wait for partner side deployment script completed, because we need to put partner side generated IAM roles into S3 bucket policy
11. run `mrpid_publisher_final_deploy.sh`
Note: This final script needs to be run after partner side deployment script!
Please use the same tag from initial script.
 * For standard `deploy`, run the following command

```
/bin/bash mrpid_publisher_final_deploy.sh deploy -r <> -t <> -a <> -p <> -b <optional>
example: /bin/bash mrpid_publisher_final_deploy.sh deploy -r us-west-2 -t your-tag-name -a 627672676272 -p 43454354533545
```

 * For standard `undeploy`, run the following command
```
/bin/bash mrpid_publisher_final_deploy.sh undeploy -r <> -t <> -a <> -p <> -b <optional>
example: /bin/bash mrpid_publisher_final_deploy.sh undeploy -r us-west-2 -t your-tag-name -a 627672676272 -p 43454354533545
```


# Notes
 * parameter tag (`-t`) cannot be too long. AWS function/variable name must have length less than or equal to 64.
 * parameters p is the required partner_account_id value from the partner-side deployment.
 * parameter b is optional.
