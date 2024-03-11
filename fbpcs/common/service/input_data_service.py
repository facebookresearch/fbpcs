# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


@dataclass
class TimestampValues:
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]


# Constants
DATASET_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
TIMESTAMP_REGEX = re.compile(r"^[0-9]+$")


"""
Determines the starting and ending timestamp range for studies and attributions
after fetching from the Graph API.
"""


class InputDataService:
    """
    The inputs are in this format:
    study_start_time: "1657090807"
    observation_end_time: "1672819207"
    """

    @classmethod
    def get_lift_study_timestamps(
        cls,
        study_start_timestamp: str,
        observation_end_timestamp: str,
        is_feature_enabled: bool,
    ) -> TimestampValues:
        try:
            if not is_feature_enabled:
                cls._logger().info("The PL timestamp validation feature is not enabled")
                return TimestampValues(
                    start_timestamp=None,
                    end_timestamp=None,
                )
            start_timestamp = None
            end_timestamp = None
            if TIMESTAMP_REGEX.match(study_start_timestamp):
                start_timestamp = study_start_timestamp
            if TIMESTAMP_REGEX.match(observation_end_timestamp):
                end_timestamp = observation_end_timestamp
            return TimestampValues(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
            )
        except Exception as e:
            cls._logger().error(
                "Error in `get_lift_study_timestamps`: "
                + f"type: {type(e)}, message: {e}"
            )
            return TimestampValues(
                start_timestamp=None,
                end_timestamp=None,
            )

    """
    The dataset_timestamp is in this format: "2022-05-01T07:00:00+0000"
    """

    @classmethod
    def get_attribution_timestamps(
        cls,
        dataset_timestamp: str,
        is_feature_enabled: bool,
    ) -> TimestampValues:
        try:
            if not is_feature_enabled:
                cls._logger().info("The PA timestamp validation feature is not enabled")
                return TimestampValues(
                    start_timestamp=None,
                    end_timestamp=None,
                )
            dataset_datetime = cls._datetime_from_string(dataset_timestamp)
            previous_day = timedelta(days=-1) + dataset_datetime
            previous_day_ts = cls._datetime_to_timestamp_string(previous_day)
            next_day = timedelta(days=1) + dataset_datetime
            next_day_ts = cls._datetime_to_timestamp_string(next_day)
            return TimestampValues(
                start_timestamp=previous_day_ts,
                end_timestamp=next_day_ts,
            )
        except Exception as e:
            cls._logger().error(
                "Error in `get_attribution_timestamps`: "
                + f"type: {type(e)}, message: {e}"
            )
            return TimestampValues(
                start_timestamp=None,
                end_timestamp=None,
            )

    @classmethod
    def _logger(cls) -> logging.Logger:
        return logging.getLogger(__name__)

    @classmethod
    def _datetime_from_string(cls, datetime_string: str) -> datetime:
        return datetime.strptime(datetime_string, DATASET_TIMESTAMP_FORMAT)

    @classmethod
    def _datetime_to_timestamp_string(cls, datetime_value: datetime) -> str:
        return str(int(datetime_value.timestamp()))
