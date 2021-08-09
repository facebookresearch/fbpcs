#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import copy
from typing import Dict, Optional

from fbpmp.pcf.structs import Metric


class DerivedMetricsCalculator(object):
    def __init__(self, metrics: Dict[str, Dict[Metric, float]]):
        self.metrics = copy.deepcopy(metrics)

    @staticmethod
    def compute_scale_factor(base_dict: Dict[Metric, float]) -> Optional[float]:
        if (
            Metric.test_population in base_dict
            and Metric.control_population in base_dict
        ):
            return (
                base_dict[Metric.test_population] / base_dict[Metric.control_population]
            )
        return None

    @staticmethod
    def compute_conversions_control_scaled(
        base_dict: Dict[Metric, float]
    ) -> Optional[float]:
        if Metric.control_conversions in base_dict and Metric.scale_factor in base_dict:
            return (
                base_dict[Metric.control_conversions] * base_dict[Metric.scale_factor]
            )
        return None

    @staticmethod
    def compute_conversions_incremental(
        base_dict: Dict[Metric, float], scale_factor: float
    ) -> Optional[float]:
        if (
            Metric.control_conversions in base_dict
            and Metric.test_conversions in base_dict
        ):
            return (
                base_dict[Metric.test_conversions]
                - base_dict[Metric.control_conversions] * scale_factor
            )
        return None

    @staticmethod
    def compute_buyers_incremental(
        base_dict: Dict[Metric, float], scale_factor: float
    ) -> Optional[float]:
        if (
            Metric.test_purchasers in base_dict
            and Metric.control_purchasers in base_dict
        ):
            return (
                base_dict[Metric.test_purchasers]
                - base_dict[Metric.control_purchasers] * scale_factor
            )
        return None

    @staticmethod
    def compute_sales_incremental(
        base_dict: Dict[Metric, float], scale_factor: float
    ) -> Optional[float]:
        if Metric.test_sales in base_dict and Metric.control_sales in base_dict:
            return (
                base_dict[Metric.test_sales]
                - base_dict[Metric.control_sales] * scale_factor
            )
        return None

    @staticmethod
    def compute_sales_delta(base_dict: Dict[Metric, float]) -> Optional[float]:
        if Metric.test_sales in base_dict and Metric.control_sales in base_dict:
            return base_dict[Metric.test_sales] - base_dict[Metric.control_sales]
        return None

    @staticmethod
    def compute_conversions_delta(base_dict: Dict[Metric, float]) -> Optional[float]:
        if (
            Metric.test_conversions in base_dict
            and Metric.control_conversions in base_dict
        ):
            return (
                base_dict[Metric.test_conversions]
                - base_dict[Metric.control_conversions]
            )
        return None

    @staticmethod
    def compute_purchasers_control_scaled(
        base_dict: Dict[Metric, float], scale_factor: float
    ) -> Optional[float]:
        if Metric.control_purchasers in base_dict:
            return base_dict[Metric.control_purchasers] * scale_factor

    @staticmethod
    def compute_purchasers_delta(base_dict: Dict[Metric, float]) -> Optional[float]:
        if (
            Metric.test_purchasers in base_dict
            and Metric.control_purchasers in base_dict
        ):
            return (
                base_dict[Metric.test_purchasers] - base_dict[Metric.control_purchasers]
            )
        return None

    def calculate_all(self) -> Dict[str, Dict[Metric, float]]:
        # Loop over each sub-result. Sub-results are independent since they
        # were constructed from mutually exclusive grouping sets
        for key, subdict in self.metrics.items():
            # scale_factor
            scale_factor = self.compute_scale_factor(subdict)
            if scale_factor is not None:
                self.metrics[key][Metric.scale_factor] = scale_factor
                self.metrics[key][Metric.control_sales_scaled] = (
                    subdict[Metric.control_sales] * scale_factor
                )
                self.metrics[key][
                    Metric.conversions_control_scaled
                ] = self.compute_conversions_control_scaled(subdict)
                self.metrics[key][
                    Metric.conversions_incremental
                ] = self.compute_conversions_incremental(subdict, scale_factor)
                self.metrics[key][
                    Metric.buyers_incremental
                ] = self.compute_buyers_incremental(subdict, scale_factor)
                self.metrics[key][
                    Metric.sales_incremental
                ] = self.compute_sales_incremental(subdict, scale_factor)
                self.metrics[key][
                    Metric.purchasers_control_scaled
                ] = self.compute_purchasers_control_scaled(subdict, scale_factor)

            # conversions_delta
            conversions_delta = self.compute_conversions_delta(subdict)
            if conversions_delta is not None:
                self.metrics[key][Metric.conversions_delta] = conversions_delta

            # sales_delta
            sales_delta = self.compute_sales_delta(subdict)
            if sales_delta is not None:
                self.metrics[key][Metric.sales_delta] = sales_delta

            # purchasers_delta
            purchasers_delta = self.compute_purchasers_delta(subdict)
            if purchasers_delta is not None:
                self.metrics[key][Metric.purchasers_delta] = purchasers_delta

        return self.metrics
