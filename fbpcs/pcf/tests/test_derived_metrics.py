#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest

from fbpcs.pcf.derived_metrics import DerivedMetricsCalculator
from fbpcs.pcf.structs import Metric


class TestDerivedMetrics(unittest.TestCase):
    def setUp(self):
        self.metrics = {
            "Overall": {
                Metric.id_match_count: 10.0,
                Metric.test_conversions: 4.0,
                Metric.control_conversions: 1.0,
                Metric.test_sales: 90.0,
                Metric.control_sales: 10.0,
                Metric.test_sales_squared: 1000.0,
                Metric.control_sales_squared: 100.0,
                Metric.test_population: 8.0,
                Metric.control_population: 2.0,
                Metric.test_purchasers: 4.0,
                Metric.control_purchasers: 1.0,
            },
            "Subgroup1": {
                Metric.id_match_count: 6.0,
                Metric.test_conversions: 3.0,
                Metric.control_conversions: 0.0,
                Metric.test_sales: 90.0,
                Metric.control_sales: 0.0,
                Metric.test_sales_squared: 800.0,
                Metric.control_sales_squared: 0.0,
                Metric.test_population: 5.0,
                Metric.control_population: 1.0,
                Metric.test_purchasers: 3.0,
                Metric.control_purchasers: 0.0,
            },
            "Subgroup2": {
                Metric.id_match_count: 4.0,
                Metric.test_conversions: 1.0,
                Metric.control_conversions: 1.0,
                Metric.test_sales: 30.0,
                Metric.control_sales: 10.0,
                Metric.test_sales_squared: 200.0,
                Metric.control_sales_squared: 100.0,
                Metric.test_population: 3.0,
                Metric.control_population: 1.0,
                Metric.test_purchasers: 1.0,
                Metric.control_purchasers: 1.0,
            },
        }

    def test_compute_scale_factor(self):
        expected = {"Overall": 4.0, "Subgroup1": 5.0, "Subgroup2": 3.0}
        for key, scale_factor in expected.items():
            res = DerivedMetricsCalculator.compute_scale_factor(self.metrics[key])
            self.assertEqual(scale_factor, res)

    def test_compute_scale_factor_missing_keys(self):
        metrics = {Metric.test_population: 4.0}
        self.assertIsNone(DerivedMetricsCalculator.compute_scale_factor(metrics))

    def test_compute_buyers_incremental(self):
        scale_factor = 3.0
        expected = {"Overall": 1.0, "Subgroup1": 3.0, "Subgroup2": -2.0}
        for key, buyers_incremental in expected.items():
            res = DerivedMetricsCalculator.compute_buyers_incremental(
                self.metrics[key], scale_factor
            )
            self.assertEqual(buyers_incremental, res)

    def test_compute_conversions_control_scaled(self):
        scale_factor = 3.0
        expected = {"Overall": 3.0, "Subgroup1": 0.0, "Subgroup2": 3.0}
        for key, conversions_control_scaled in expected.items():
            self.metrics[key][Metric.scale_factor] = scale_factor
            res = DerivedMetricsCalculator.compute_conversions_control_scaled(
                self.metrics[key]
            )
            self.assertEqual(conversions_control_scaled, res)

    def test_compute_conversions_incremental_scaled(self):
        scale_factor = 2.0
        expected = {"Overall": 2.0, "Subgroup1": 3.0, "Subgroup2": -1.0}
        for key, conversions_incremental_scaled in expected.items():
            res = DerivedMetricsCalculator.compute_conversions_incremental(
                self.metrics[key], scale_factor
            )
            self.assertEqual(conversions_incremental_scaled, res)

    def test_compute_conversions_incremental_missing_keys(self):
        metrics = [
            {},
            {Metric.test_conversions: 1.0},
            {Metric.control_conversions: 1.0},
        ]
        for metric in metrics:
            self.assertIsNone(
                DerivedMetricsCalculator.compute_conversions_incremental(metric, 1.0)
            )

    def test_compute_buyers_control_scaled(self):
        scale_factor = 3.0
        expected = {"Overall": 3.0, "Subgroup1": 0.0, "Subgroup2": 3.0}
        for key, purchasers_control_scaled in expected.items():
            res = DerivedMetricsCalculator.compute_purchasers_control_scaled(
                self.metrics[key], scale_factor
            )
            self.assertEqual(purchasers_control_scaled, res)

    def test_compute_buyers_incremental_missing_keys(self):
        metrics = [
            {},
            {Metric.test_purchasers: 1.0},
            {Metric.control_purchasers: 1.0},
            {Metric.test_purchasers: 1.0, Metric.scale_factor: 1.0},
            {Metric.control_purchasers: 1.0, Metric.scale_factor: 1.0},
        ]
        for metric in metrics:
            self.assertIsNone(
                DerivedMetricsCalculator.compute_buyers_incremental(metric, 1.0)
            )

    def test_compute_sales_incremental_scaled(self):
        scale_factor = 3.0
        expected = {"Overall": 60.0, "Subgroup1": 90.0, "Subgroup2": 0.0}
        for key, sales_incremental_scaled in expected.items():
            res = DerivedMetricsCalculator.compute_sales_incremental(
                self.metrics[key], scale_factor
            )
            self.assertEqual(sales_incremental_scaled, res)

    def test_compute_sales_incremental_missing_keys(self):
        metrics = [{}, {Metric.test_sales: 1.0}, {Metric.control_sales: 1.0}]
        for metric in metrics:
            self.assertIsNone(
                DerivedMetricsCalculator.compute_sales_incremental(metric, 1.0)
            )

    def test_compute_sales_delta(self):
        expected = {"Overall": 80.0, "Subgroup1": 90.0, "Subgroup2": 20.0}
        for key, sales_delta in expected.items():
            res = DerivedMetricsCalculator.compute_sales_delta(self.metrics[key])
            self.assertEqual(sales_delta, res)

    def test_compute_sales_delta_missing_keys(self):
        metrics = {Metric.test_sales: 90.0}
        self.assertIsNone(DerivedMetricsCalculator.compute_sales_delta(metrics))

    def test_compute_conversions_delta(self):
        expected = {"Overall": 3.0, "Subgroup1": 3.0, "Subgroup2": 0.0}
        for key, scale_factor in expected.items():
            res = DerivedMetricsCalculator.compute_conversions_delta(self.metrics[key])
            self.assertEqual(scale_factor, res)

    def test_compute_conversions_delta_missing_keys(self):
        metrics = {Metric.test_conversions: 4.0}
        self.assertIsNone(DerivedMetricsCalculator.compute_conversions_delta(metrics))

    def test_compute_purchasers_delta(self):
        expected = {"Overall": 3.0, "Subgroup1": 3.0, "Subgroup2": 0.0}
        for key, purchasers_delta in expected.items():
            res = DerivedMetricsCalculator.compute_purchasers_delta(self.metrics[key])
            self.assertEquals(purchasers_delta, res)

    def test_compute_purchasers_delta_missing_keys(self):
        metrics = {Metric.test_purchasers: 3.0}
        self.assertIsNone(DerivedMetricsCalculator.compute_purchasers_delta(metrics))

    def test_calculate_all(self):
        calc = DerivedMetricsCalculator(self.metrics)
        res = calc.calculate_all()
        for subdict in res.values():
            self.assertIn(Metric.scale_factor, subdict)
            self.assertIn(Metric.buyers_incremental, subdict)
            self.assertIn(Metric.conversions_control_scaled, subdict)
            self.assertIn(Metric.control_sales_scaled, subdict)
            self.assertIn(Metric.sales_delta, subdict)
            self.assertIn(Metric.conversions_delta, subdict)
            self.assertIn(Metric.purchasers_control_scaled, subdict)
            self.assertIn(Metric.purchasers_delta, subdict)
            self.assertIn(Metric.conversions_incremental, subdict)
            self.assertIn(Metric.sales_incremental, subdict)
