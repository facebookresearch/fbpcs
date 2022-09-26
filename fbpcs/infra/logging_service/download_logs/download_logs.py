# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import os
import tempfile
from concurrent.futures import as_completed, ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from typing import Callable, Dict, List, Optional

from fbpcs.infra.logging_service.download_logs.cloud.aws_cloud import AwsCloud
from fbpcs.infra.logging_service.download_logs.utils.utils import (
    ContainerDetails,
    DataInfraLambda,
    DeploymentLogFiles,
    StringFormatter,
    Utils,
)


class AwsContainerLogs(AwsCloud):
    """
    Fetches container logs from the cloudwatch
    """

    ARN_PARSE_LENGTH = 6
    S3_LOGGING_FOLDER = "logging"
    COMPUTATION_RUN_CONTAINER_LOG_FOLDER = "container_logs"
    DEPLOYMENT_LOGS_FOLDER = "deployment_logs"
    KINESIS_LOGS_FOLDER = "kinesis_logs"
    LAMBDA_LOGS_FOLDER = "lambda_logs"
    GLUE_LOGS_FOLDER = "glue_logs"
    KINESIS_ERROR_LOGS = "kinesis_error_logs"
    KINESIS_CONFIG_LOGS = "kinesis_config_logs"
    DEFAULT_DOWNLOAD_LOCATION = "/tmp"
    MAX_THREADS = 500
    THREADS_PER_CORE = 20

    def __init__(
        self,
        tag_name: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_region: Optional[str] = None,
        deployment_tag: Optional[str] = None,
    ) -> None:
        super().__init__(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_region=aws_region,
        )
        self.utils = Utils()
        self.tag_name: str = tag_name
        self.deployment_tag = deployment_tag
        self.containers_without_logs: List[str] = []
        self.containers_download_logs_failed: List[str] = []
        self.write_to_file_lock = Lock()

    def upload_logs_to_s3_from_cloudwatch(
        self,
        s3_bucket_name: str,
        container_arn_list: List[str],
        copy_debug_logs: bool = False,
        copy_debug_logs_location: str = DEFAULT_DOWNLOAD_LOCATION,
        **kwargs: Dict[str, bool],
    ) -> None:
        """
        Umbrella function to call other functions to upload the logs from cloudwatch to S3

        Folder structure

        [S3 Bucket]
            -> [folder name "logging"]
                -> [folder name in format {tag_name}]
                    -> [exported logs from each container]

        Args:
            s3_bucket_name (string): Name of the s3 bucket where logs will be stored
            container_arn_list (string): List of container arn to get log group and log stream names
        Returns:
            None
        """

        enable_data_pipeline_logs = kwargs.get("enable_data_pipeline_logs", True)
        enable_deployment_logs = kwargs.get("enable_deployment_logs", True)

        # take only unique entries of container ARNs to avoid downloading duplicate logs
        container_arn_list = list(set(container_arn_list))

        # verify if s3 bucket exists
        self.verify_s3_bucket(s3_bucket_name=s3_bucket_name)

        # create logging folder
        self.create_s3_folder(
            bucket_name=s3_bucket_name,
            folder_name=f"{self.S3_LOGGING_FOLDER}/",
        )

        # creating temp directory to store logs locally
        with tempfile.TemporaryDirectory(prefix=self.tag_name) as tempdir:
            self.log.info(f"Created temperory directory to store logs {tempdir}")

            local_folder_location = self.utils.string_formatter(
                StringFormatter.FILE_LOCATION, tempdir, self.tag_name
            )
            zipped_file_path = self.utils.string_formatter(
                StringFormatter.LOCAL_ZIP_FOLDER_LOCATION, local_folder_location
            )

            zipped_folder = self.utils.string_formatter(
                StringFormatter.ZIPPED_FOLDER_NAME, self.tag_name
            )

            s3_file_path = self.utils.string_formatter(
                StringFormatter.FILE_LOCATION, self.S3_LOGGING_FOLDER, zipped_folder
            )

            # store logs in local
            self.utils.create_folder(folder_location=local_folder_location)

            if enable_data_pipeline_logs:
                # upload computation run container logs
                self._upload_computation_run_container_logs(
                    container_arn_list=container_arn_list,
                    local_log_folder_location=local_folder_location,
                )

                # upload kinesis firehose error and config logs
                self._upload_kinesis_logs(
                    local_log_folder_location=local_folder_location
                )

                # upload data infra lambda logs
                self._upload_lambda_logs(
                    local_log_folder_location=local_folder_location
                )

                # upload data infra glue logs
                self._prepare_glue_logs(local_log_folder_location=local_folder_location)

            if enable_deployment_logs:
                self._upload_deployment_logs(
                    local_log_folder_location=local_folder_location
                )

            # compressing the folder before uploading it to S3
            self.log.info("Compressing downloaded logs folder")
            self.utils.compress_downloaded_logs(folder_location=local_folder_location)
            self.log.info("Compressed download log folder.")

            self.upload_file_to_s3(
                s3_bucket_name=s3_bucket_name,
                s3_file_path=s3_file_path,
                file_name=zipped_file_path,
            )

            if copy_debug_logs:
                self.copy_logs_for_debug(
                    source=zipped_file_path, destination=copy_debug_logs_location
                )

            self.log.info("Removing logs locally.")

    def _upload_computation_run_container_logs(
        self, container_arn_list: List[str], local_log_folder_location: str
    ) -> None:
        """
        Uploads computation run stage container logs from AWS cloudwatch to S3
        """
        # Call threading function to download logs locally and upload to S3
        computaion_run_container_log_folder = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            local_log_folder_location,
            self.COMPUTATION_RUN_CONTAINER_LOG_FOLDER,
        )

        self.utils.create_folder(folder_location=computaion_run_container_log_folder)
        self.run_threaded_download(
            func=self.store_container_logs_locally,
            container_arn_list=container_arn_list,
            local_folder_location=computaion_run_container_log_folder,
        )

        # List all the containers with no cloudwatch logs
        self.log_containers_without_logs()

        # List all containers for which download failed because of thread crash
        # TODO: T123687467: add retry logic to download logs for the failed cases of download
        self.log_containers_download_log_failed()

    def _upload_deployment_logs(self, local_log_folder_location: str) -> None:
        # check if file exists
        deployment_logs = DeploymentLogFiles.list()
        deployment_logs_exists = filter(lambda x: Path(x).is_file(), deployment_logs)

        # create folder
        deployment_log_folder = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            local_log_folder_location,
            self.DEPLOYMENT_LOGS_FOLDER,
        )
        self.utils.create_folder(folder_location=deployment_log_folder)

        # copy files to folder
        for file_path in deployment_logs_exists:
            file_name = self.utils.get_file_name_from_path(file_path=file_path)
            file_location = self.utils.string_formatter(
                StringFormatter.FILE_LOCATION,
                deployment_log_folder,
                file_name,
            )

            file_name = self.utils.copy_file(
                source=file_path, destination=file_location
            )

    def _upload_kinesis_logs(self, local_log_folder_location: str) -> None:
        if not self.deployment_tag:
            self.log.error(
                "Couldn't get the deployment tag to fetch the AWS Kinesis resources."
            )
            return

        kinesis_firehose_stream_name = self.utils.string_formatter(
            StringFormatter.KINESIS_FIREHOSE_DELIVERY_STREAM, self.deployment_tag
        )

        # create kinesis log folder
        kinesis_log_folder = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            local_log_folder_location,
            self.KINESIS_LOGS_FOLDER,
        )
        self.utils.create_folder(folder_location=kinesis_log_folder)

        # fetch Kinesis configs
        response = self.get_kinesis_firehose_streams(
            kinesis_firehose_stream_name=kinesis_firehose_stream_name
        )
        cloudwatch_config = self.get_kinesis_firehose_config(response=response)

        # check if logs are enabled
        if cloudwatch_config.get("Enabled", False):
            cloudwatch_log_group = cloudwatch_config.get("LogGroupName", "")
            cloudwatch_log_stream = cloudwatch_config.get("LogStreamName", "")

            if self._verify_log_group(
                log_group_name=cloudwatch_log_group
            ) and self._verify_log_stream(
                log_group_name=cloudwatch_log_group,
                log_stream_name=cloudwatch_log_stream,
            ):
                messages = self.get_cloudwatch_logs(
                    log_group_name=cloudwatch_log_group,
                    log_stream_name=cloudwatch_log_stream,
                )
                kinesis_error_log_file_location = self.utils.string_formatter(
                    StringFormatter.FILE_LOCATION,
                    kinesis_log_folder,
                    self.KINESIS_ERROR_LOGS,
                )
                self.utils.create_file(
                    file_location=kinesis_error_log_file_location,
                    content=messages,
                )

        # copy kinesis logs config
        kinesis_config_log_file_location = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            kinesis_log_folder,
            self.KINESIS_CONFIG_LOGS,
        )
        self.utils.create_file(
            file_location=kinesis_config_log_file_location,
            content=response,
        )

    def _upload_lambda_logs(self, local_log_folder_location: str) -> None:
        """
        Uploads the data infra lambda logs for both semi-automated and fully automated pipelines
        Logs are compressed and uploaded along with container logs and other data infra resources logs
        """
        if not self.deployment_tag:
            self.log.error(
                "Couldn't get the deployment tag to fetch the AWS Lambda resources."
            )
            return

        # Create folder for lambda logs
        lambda_log_folder = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            local_log_folder_location,
            self.LAMBDA_LOGS_FOLDER,
        )
        self.utils.create_folder(folder_location=lambda_log_folder)

        # get the lambda names
        data_infra_lambda_logs = DataInfraLambda.list()
        for data_infra_lambda in data_infra_lambda_logs:
            data_infra_lambda_name = data_infra_lambda.format(self.deployment_tag)
            lambda_log_group = self.utils.string_formatter(
                StringFormatter.LAMBDA_LOG_GROUP_NAME, data_infra_lambda_name
            )
            log_stream_name = self.get_latest_cloudwatch_log(
                log_group_name=lambda_log_group
            )
            messages = self.get_cloudwatch_logs(
                log_group_name=lambda_log_group,
                log_stream_name=log_stream_name,
            )
            lambda_log_file_location = self.utils.string_formatter(
                StringFormatter.FILE_LOCATION, lambda_log_folder, data_infra_lambda_name
            )
            self.utils.create_file(
                file_location=lambda_log_file_location,
                content=messages,
            )

    def _prepare_glue_logs(self, local_log_folder_location: str) -> None:
        """
        Upload glue crawler and ETL logs
        """

        if not self.deployment_tag:
            self.log.error(
                "Couldn't get the deployment tag to fetch the AWS Glue resources."
            )
            return

        # Create folder for glue logs
        glue_log_folder = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            local_log_folder_location,
            self.GLUE_LOGS_FOLDER,
        )
        self.utils.create_folder(folder_location=glue_log_folder)

        crawler_name = self.utils.string_formatter(
            StringFormatter.GLUE_CRAWLER_NAME, self.deployment_tag
        )

        # fetch crawler configs and metrics
        response_crawler_config = self.get_glue_crawler_config(
            glue_crawler_name=crawler_name
        )
        response_crawler_metric = self.get_glue_crawler_metrics(
            glue_crawler_name=crawler_name
        )
        response_crawler_config.update(response_crawler_metric)

        # copy glue crawler config
        glue_crawler_log_file_location = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            glue_log_folder,
            crawler_name,
        )
        self.utils.create_file(
            file_location=glue_crawler_log_file_location,
            content=response_crawler_config,
        )

        glue_etl_name = self.utils.string_formatter(
            StringFormatter.GLUE_ETL_NAME, self.deployment_tag
        )

        # fetch glue ETL configs and metrics
        response_glue_etl_config = self.get_glue_etl_job_details(
            glue_etl_name=glue_etl_name
        )
        response_glue_etl_metrics = self.get_glue_etl_job_run_details(
            glue_etl_name=glue_etl_name
        )
        response_glue_etl_config.update(response_glue_etl_metrics)

        # copy glue etl config
        glue_etl_log_file_location = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION,
            glue_log_folder,
            glue_etl_name,
        )
        self.utils.create_file(
            file_location=glue_etl_log_file_location,
            content=response_glue_etl_config,
        )

    def _parse_container_arn(self, container_arn: Optional[str]) -> ContainerDetails:
        """
        Parses container arn to get the container name and id needed to derive log name and log stream
        Example ARN looks like:
        arn:aws:ecs:fake-region:123456789:task/fake-container-name/1234abcdef56789
        Args:
            container_arn (String): Container ARN
        Returns: String, String, String
        """
        service_name, container_name, container_id = None, None, None

        if container_arn is None:
            # TODO T122315363: Raise more specific exception
            raise Exception(
                "Container arn is missing. Please check the arn of the container"
            )

        container_arn_list = container_arn.split(":")

        if len(container_arn_list) < self.ARN_PARSE_LENGTH:
            self.log.error("Container ARN is not in the right format.")

        try:
            self.log.info("Getting service name and task ID from container arn")
            service_name = container_arn_list[2]
            task_id = container_arn_list[5]
            container_name, container_id = self._get_container_name_id(task_id=task_id)
        except IndexError as error:
            # TODO T122315363: Raise more specific exception
            raise Exception(f"Error in getting service name and task ID: {error}")

        return ContainerDetails(
            service_name=service_name,
            container_name=container_name,
            container_id=container_id,
        )

    def _get_container_name_id(self, task_id: str) -> List[str]:
        """
        Fetches container name and container ID from the task ID
        Args:
            task_id (String): task of the container extracted from contianer arn.
        Return: String, String
        """
        container_name, container_id = None, None

        task_id_list = task_id.split("/")

        if len(task_id_list) < 3:
            self.log.error("Task ID is not in the right format.")

        try:
            self.log.info("Getting container name from the task ID")
            container_name = task_id_list[1].replace("-cluster", "-container")
            container_id = task_id_list[2]
        except IndexError as error:
            # TODO T122315363: Raise more specific exception
            raise Exception(
                f"Error in getting container name and container ID: {error}"
            )

        # TODO T122316416: Return dataclass object instead of list
        return [container_name, container_id]

    def log_containers_without_logs(self) -> None:
        """
        Lists containers with no logs in cloudwatch
        """
        if len(self.containers_without_logs) == 0:
            self.log.info("Found logs for all the containers.")
            return

        self.log.error("Couldn't find logs for the following containers..")
        for container in self.containers_without_logs:
            self.log.error(f"Container ARN: {container}")

    def log_containers_download_log_failed(self) -> None:
        """
        List contianers for which download logs failed because of thread crash
        """
        if len(self.containers_download_logs_failed) == 0:
            self.log.info("Downloaded logs for all the available containers")
            return

        arns = ", ".join(self.containers_without_logs)
        self.log.error(f"Couldn't find logs for the following_containers: {arns}")

    def store_container_logs_locally(
        self, local_folder_location: str, container_arn: str
    ) -> None:
        # fetch logs
        self.log.info(
            "Getting service name, container name and container ID from continer arn"
        )
        container_details = self._parse_container_arn(container_arn=container_arn)
        service_name = container_details.service_name
        container_name = container_details.container_name
        container_id = container_details.container_id

        log_group_name = self.utils.string_formatter(
            StringFormatter.LOG_GROUP, service_name, container_name
        )
        log_stream_name = self.utils.string_formatter(
            StringFormatter.LOG_STREAM, service_name, container_name, container_id
        )
        local_file_location = self.utils.string_formatter(
            StringFormatter.FILE_LOCATION, local_folder_location, container_id
        )

        # Check if logs are missing for any containers
        if not self._verify_log_group(
            log_group_name=log_group_name
        ) or not self._verify_log_stream(
            log_group_name=log_group_name, log_stream_name=log_stream_name
        ):
            self.containers_without_logs.append(container_arn)
        else:
            message_events = self.get_cloudwatch_logs(
                log_group_name=log_group_name,
                log_stream_name=log_stream_name,
                container_arn=container_arn,
            )

            self.log.info(
                f"Creating file to store log locally in location {local_file_location}"
            )
            with self.write_to_file_lock:
                self.utils.create_file(
                    file_location=local_file_location,
                    content=message_events,
                )

    def run_threaded_download(
        self,
        func: Callable[[str, str], None],
        container_arn_list: List[str],
        local_folder_location: str,
        num_threads: int = MAX_THREADS,
    ) -> List[str]:
        core_count = (
            os.cpu_count() or self.MAX_THREADS
        )  # os.cpu_count returns None if undertermined
        num_threads = min(core_count * self.THREADS_PER_CORE, num_threads)
        res = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            future_to_container = {
                executor.submit(func, local_folder_location, container): container
                for container in container_arn_list
            }
            for future in as_completed(future_to_container):
                container = future_to_container[future]
                try:
                    res.append(future.result())
                except Exception as exc:
                    self.containers_download_logs_failed.append(container)
                    self.log.warning(
                        f"Downloading container {container} log generated an exception: {exc}"
                    )
        return res

    def copy_logs_for_debug(
        self, source: str, destination: str = DEFAULT_DOWNLOAD_LOCATION
    ) -> None:
        """
        Copy logs from temp dir for local debugging
        """
        self.log.info(f"Copying compressed logs to {destination}")
        self.utils.copy_file(source=source, destination=destination)
        self.log.info(f"Copied compressed logs to {destination}")
