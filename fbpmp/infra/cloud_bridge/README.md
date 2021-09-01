This README guide will show you how to run `deploy.sh`.

1. Download the `infra` directory
    * run the following command

        git clone https://github.com/facebookresearch/fbpcs.git
2. Install docker and Java
    * https://hub.docker.com/editions/community/docker-ce-desktop-mac
    * https://java.com/en/download/help/download_options.html
3. Change to `cloud_bridge` directory
    * run the following command

        cd fbpcs/fbpmp/infra/cloud_bridge
4. build the image
    * run the following command

        make image-build
5. find your image tag/id:
    * run one of the following commands

        docker image ls
        `or`
        docker images cloudbridge-private_lift-server
6. given the right docker image tag/id, do `docker run`
    * run the following command

        docker run -it <image-tag> /bin/sh
7. initiate helper function
    * run the following command

        /bin/sh ./terraform_deployment/deploy.sh --help
8. create environment variables on AWS credentials (while be removed eventually)
    * run the following command

        export AWS_ACCESS_KEY_ID=<YOUR_OWN_AWS_ACCESS_KEY>
        export AWS_SECRET_ACCESS_KEY=<YOUR_OWN_AWS_SECRET_ACCESS_KEY>
        export TF_LOG=DEBUG
        export TH_LOG_PATH=/tmp/deploy.log
9. run `deploy.sh`
    * run the following command

        /bin/sh ./terraform_deployment/deploy.sh -r <> -a <> -p <> -v <> -s <> -d <> -t <>
