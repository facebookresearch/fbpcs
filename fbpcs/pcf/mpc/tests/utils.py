#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import pathlib
import random
import tempfile
import unittest
from typing import Any, Dict, Iterator, List, Optional, Tuple

from fbpcs.pcf.games import (
    ConversionLift,
    ConverterLift,
    SecretShareConversionLift,
    SecretShareConverterLift,
)
from fbpcs.pcf.mpc.base import MPCFramework
from fbpcs.pcf.structs import Game, InputColumn, Metric, Role


# 10 second offset of purchase_ts when comparing against opportunity_ts
CONST_TS_OFFSET = 10
DEFAULT_NUM_RECORDS = 10
OPPORTUNITY_RATE = 0.9
TEST_RATE = 0.5
PURCHASE_RATE = 0.5
NUM_CONVERSIONS_PER_USER = 4
EPOCH = 1546300800


class MPCTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mpc: Optional[MPCFramework] = None
        self.opportunity_rate = OPPORTUNITY_RATE
        self.test_rate = TEST_RATE
        self.purchase_rate = PURCHASE_RATE
        self.publisher_data: Optional[Dict[InputColumn, List[Any]]] = None
        self.partner_data: Optional[Dict[InputColumn, List[Any]]] = None
        self.expected_metrics: Optional[Dict[Metric, int]] = None
        self.publisher_mpc: Optional[MPCFramework] = None
        self.partner_mpc: Optional[MPCFramework] = None

    def _verify_csv(self, filepath: pathlib.Path, expected_values: List[Any]) -> None:
        with open(filepath) as f:
            lines = [line.strip() for line in f.readlines()]
        for line, expected in zip(lines, expected_values):
            self.assertEqual(line, str(expected))

    def _gen_random_output_file(self, filepath: pathlib.Path) -> Dict[Metric, int]:
        # Since this is just a unit test, it's okay if the actual values are
        # nonsensical in a Lift context; we're just testing for correct parsing.
        mpc = self.mpc
        assert mpc is not None
        game = mpc.game
        assert game is not None

        result = {metric: random.randint(0, 100) for metric in game.output_metrics}
        with open(filepath, "w") as f:
            # Don't loop over the dictionary since order is important
            for metric in game.output_metrics:
                value = result[metric]
                f.write(f"{value}\n")
        return result

    def _faked_data(self, row_num: int, header: List[InputColumn]) -> List[Any]:
        has_purchase = 1 if random.random() < PURCHASE_RATE else 0
        gens = {
            InputColumn.id_: lambda: row_num,
            InputColumn.row_count: lambda: row_num,
            InputColumn.opportunity: lambda: 1
            if random.random() < OPPORTUNITY_RATE
            else 0,
            InputColumn.test_flag: lambda: 1 if random.random() < TEST_RATE else 0,
            InputColumn.opportunity_timestamp: lambda: random.randint(
                EPOCH, EPOCH + 100
            ),
            # opportunity_timestamps is an array of values for each data row
            # For testing we can assume an array of size 5
            InputColumn.opportunity_timestamps: lambda: [
                random.randint(EPOCH, EPOCH + 100) for _ in range(5)
            ],
            InputColumn.event_timestamp: lambda: random.randint(EPOCH, EPOCH + 100),
            # event_timestamps can be an array of all zeroes, valid timestamps
            # preceded by zeroes, or all non-zeroes
            InputColumn.event_timestamps: lambda: sorted(
                [
                    has_purchase * random.randint(EPOCH, EPOCH + 100)
                    for _ in range(row_num % NUM_CONVERSIONS_PER_USER + 1)
                ]
                + [0]
                * (NUM_CONVERSIONS_PER_USER - row_num % NUM_CONVERSIONS_PER_USER - 1)
            ),
            InputColumn.value: lambda: random.randint(1, 100)
            if random.random() < PURCHASE_RATE
            else 0,
            # values can be an array of all zeroes, non-zeroes preceded
            # by zeroes, or all non-zeroes. The number of non-zeroes would
            # match that of the event_timestamps column.
            InputColumn.values: lambda: [
                has_purchase * random.randint(1, 100)
                if i
                >= (NUM_CONVERSIONS_PER_USER - row_num % NUM_CONVERSIONS_PER_USER - 1)
                else 0
                for i in range(NUM_CONVERSIONS_PER_USER)
            ],
            InputColumn.value_squared: lambda: random.randint(1, 100)
            if random.random() < PURCHASE_RATE
            else 0,
            InputColumn.purchase_flag: lambda: 1
            if random.random() < PURCHASE_RATE
            else 0,
            # The feature column is supposed to be generic, but for testing, we
            # can assume it is a simple binary feature.
            InputColumn.features: lambda: random.randint(0, 1),
        }
        return [gens[column]() for column in header]

    @staticmethod
    def _make_secret_share_from_data_array(
        data: List[int],
    ) -> Tuple[List[int], List[int]]:
        a_shares = [random.randint(-100, 100) for _ in data]
        b_shares = [val - a for val, a in zip(data, a_shares)]
        return a_shares, b_shares

    @staticmethod
    def _make_secret_share_for_shared_data(
        series: List[Any],
    ) -> Tuple[List[Any], List[Any]]:

        # series could be a one-dimensional array of values
        # or could be an array of arrays for the likes of opportunity_timestamps:
        # id_,test_flag,opportunity_timestamps,event_timestamp,value,value_squared
        # 2135,-6713,[5173, 6570, 9377],959,9343,8870
        #
        # For values which are arrays itself, each element needs to be divided
        # between publisher and partner in the same way we handle array of scalars
        a_shares = []
        b_shares = []
        # column is an array. series represents an array of arrays
        # each element needs to be translated into publisher and partner
        # value arrays
        if len(series) != 0 and isinstance(series[0], list):
            result = [
                MPCTestCase._make_secret_share_from_data_array(val) for val in series
            ]
            a_shares, b_shares = map(list, zip(*result))
        # column is a scalar column. series represents an array
        else:
            a_shares, b_shares = MPCTestCase._make_secret_share_from_data_array(series)
        return a_shares, b_shares

    @staticmethod
    def _make_secret_share_from_data(
        data: Dict[InputColumn, List[Any]], clear_columns: List[InputColumn]
    ) -> Tuple[Dict[InputColumn, List[Any]], Dict[InputColumn, List[Any]]]:
        a_share: Dict[InputColumn, List[Any]] = {}
        b_share: Dict[InputColumn, List[Any]] = {}
        for column, series in data.items():
            # If column belongs to the clear column list
            # both publisher and partner contain the unchanged data
            if column in clear_columns:
                a_shares = series
                b_shares = series
            # If column is secret_shared, split the data accordingly
            else:
                a_shares, b_shares = MPCTestCase._make_secret_share_for_shared_data(
                    series
                )
            a_share[column] = a_shares
            b_share[column] = b_shares
        return a_share, b_share

    def _get_input_csv_from_local(
        self, game: Game, role: Role, filepath: pathlib.Path
    ) -> Tuple[Dict[InputColumn, List[int]], pathlib.Path]:
        # NOTE: If you use this function, make sure to comment out any
        # references to "shutil.rmtree" since you likely don't want to also
        # delete the local file at the conclusion of your testing
        header = game.input_columns[role]

        data: Dict[InputColumn, List[int]] = {column: [] for column in header}
        with open(filepath) as f:
            # Discard header
            f.readline()
            for line in f:
                fields = [int(f) for f in line.strip().split(",")]
                for col, val in zip(header, fields):
                    data[col].append(val)
        return data, filepath

    @staticmethod
    def _get_header_for_csv(header: List[InputColumn]) -> List[str]:
        strs: List[str] = []
        for col in header:
            # For ConversionLift, we need a special case: for the
            # features column, output like feature_a so the C++ game will
            # actually understand how to parse it.
            if col == InputColumn.features:
                strs.append("feature_a")
            else:
                strs.append(str(col))
        return strs

    @staticmethod
    def _write_data_to_tmp_file(
        header: List[InputColumn], data: Dict[InputColumn, List[Any]]
    ) -> pathlib.Path:
        csv_id = random.randint(0, 2 ** 32)
        tempdir = tempfile.mkdtemp()
        input_path = pathlib.Path(tempdir) / f"{csv_id}.csv"
        num_records = max(len(series) for series in data.values())
        with open(input_path, "w") as f:
            # For ConversionLift, we need a special case: for the
            # features column, output like feature_a so the C++ game will
            # actually understand how to parse it.
            f.write(",".join(MPCTestCase._get_header_for_csv(header)))
            f.write("\n")
            for i in range(num_records):
                values = [data[col][i] for col in header]
                f.write(",".join(str(x) for x in values))
                f.write("\n")
        return input_path

    def _gen_input_data(
        self,
        game: Game,
        role: Role,
        num_records: int = DEFAULT_NUM_RECORDS,
        column_overrides: Optional[Dict[InputColumn, Iterator[Any]]] = None,
    ) -> Dict[InputColumn, List[Any]]:
        header = game.input_columns[role]
        if column_overrides is None:
            column_overrides = {}
        # Generate input data
        data: Dict[InputColumn, List[Any]] = {column: [] for column in header}
        for i in range(num_records):
            values = self._faked_data(i, header)
            for column, iterator in column_overrides.items():
                values[header.index(column)] = next(iterator)
            for i, col in enumerate(header):
                data[col].append(values[i])
        return data

    def _make_input_csv(
        self,
        game: Game,
        role: Role,
        num_records: int = DEFAULT_NUM_RECORDS,
        column_overrides: Optional[Dict[InputColumn, Iterator[Any]]] = None,
    ) -> Tuple[Dict[InputColumn, List[int]], pathlib.Path]:
        header = game.input_columns[role]
        data = self._gen_input_data(game, role, num_records, column_overrides)
        input_path = self._write_data_to_tmp_file(header, data)
        return data, input_path

    def _make_secret_share_input_csvs(
        self,
        game: Game,
        num_records: int = DEFAULT_NUM_RECORDS,
        column_overrides: Optional[Dict[InputColumn, Iterator[Any]]] = None,
    ) -> Tuple[
        Tuple[Dict[InputColumn, List[int]], pathlib.Path],
        Tuple[Dict[InputColumn, List[int]], pathlib.Path],
    ]:
        # For now test_flag is the only column in the clear
        clear_columns = [InputColumn.test_flag]
        # For secret share games, Role is irrelevant
        header = game.input_columns[Role.PUBLISHER]
        data = self._gen_input_data(game, Role.PUBLISHER, num_records, column_overrides)
        a_share, b_share = self._make_secret_share_from_data(data, clear_columns)
        a_path = self._write_data_to_tmp_file(header, a_share)
        b_path = self._write_data_to_tmp_file(header, b_share)
        return ((a_share, a_path), (b_share, b_path))

    def _compute_expected_metrics(self, game: Game) -> Dict[Metric, int]:
        if game == ConversionLift:
            return self._compute_conversion_lift_metrics()
        elif game == ConverterLift:
            return self._compute_conversion_lift_metrics(is_converter=True)
        elif game == SecretShareConversionLift:
            return self._compute_secret_share_conversion_lift_metrics()
        elif game == SecretShareConverterLift:
            return self._compute_secret_share_conversion_lift_metrics(is_converter=True)
        else:
            raise Exception(f"Metrics computation not implemented for {game}")

    def _compute_conversion_lift_metrics(
        self, is_converter: bool = False
    ) -> Dict[Metric, int]:
        test_pop = 0
        control_pop = 0
        test_events = 0
        control_events = 0
        if not is_converter:
            test_sales = 0
            control_sales = 0
            test_sq = 0
            control_sq = 0

        # Aliasing to make lines shorter and easier to read
        pub = self.publisher_data
        partner = self.partner_data
        assert pub is not None
        assert partner is not None
        for i in range(len(pub[InputColumn.id_])):
            # Opportunity is an optional column. If it isn't in the input
            # data, we assume it's set to true (1).
            if InputColumn.opportunity not in pub or pub[InputColumn.opportunity][i]:
                if is_converter:
                    event_timestamps = [partner[InputColumn.event_timestamp][i]]
                    num_convs = 1
                else:
                    num_convs = min(
                        len(partner[InputColumn.event_timestamps][i]),
                        NUM_CONVERSIONS_PER_USER,
                    )
                    event_timestamps = partner[InputColumn.event_timestamps][i]

                if pub[InputColumn.test_flag][i]:
                    test_pop += 1
                else:
                    control_pop += 1

                test_sales_sub_sum = 0
                control_sales_sub_sum = 0
                for j in range(num_convs):
                    valid_ts = (
                        pub[InputColumn.opportunity_timestamp][i]
                        < event_timestamps[j] + CONST_TS_OFFSET
                    )
                    if pub[InputColumn.test_flag][i]:
                        if valid_ts:
                            test_events += 1
                            if not is_converter:
                                test_sales_sub_sum += partner[InputColumn.values][i][j]
                    else:
                        if valid_ts:
                            control_events += 1
                            if not is_converter:
                                control_sales_sub_sum += partner[InputColumn.values][i][
                                    j
                                ]
                if not is_converter:
                    test_sales += test_sales_sub_sum
                    test_sq += test_sales_sub_sum ** 2
                    control_sales += control_sales_sub_sum
                    control_sq += control_sales_sub_sum ** 2
        res = {
            Metric.test_population: test_pop,
            Metric.control_population: control_pop,
        }

        if is_converter:
            res.update(
                {
                    Metric.test_purchasers: test_events,
                    Metric.control_purchasers: control_events,
                }
            )
        else:
            # Conversion lift uses _conversions instead of _purchasers
            res.update(
                {
                    Metric.test_conversions: test_events,
                    Metric.control_conversions: control_events,
                    # pyre-fixme[61]: `test_sales` may not be initialized here.
                    Metric.test_sales: test_sales,
                    # pyre-fixme[61]: `control_sales` may not be initialized here.
                    Metric.control_sales: control_sales,
                    # pyre-fixme[61]: `test_sq` may not be initialized here.
                    Metric.test_sales_squared: test_sq,
                    # pyre-fixme[61]: `control_sq` may not be initialized here.
                    Metric.control_sales_squared: control_sq,
                }
            )
        return res

    def _compute_secret_share_conversion_lift_metrics(
        self, is_converter: bool = False
    ) -> Dict[Metric, int]:
        test_pop = 0
        control_pop = 0
        test_events = 0
        control_events = 0
        if not is_converter:
            test_sales = 0
            control_sales = 0
            test_sq = 0
            control_sq = 0

        # Aliasing to make lines shorter and easier to read
        pub = self.publisher_data
        partner = self.partner_data
        assert pub is not None
        assert partner is not None
        for i in range(len(pub[InputColumn.id_])):
            # Combines opportunity_timestamps array from publisher
            # and partner, and go row by row to compute minimum
            opp_ts = min(
                pub_ts + partner_ts
                for pub_ts, partner_ts in zip(
                    pub[InputColumn.opportunity_timestamps][i],
                    partner[InputColumn.opportunity_timestamps][i],
                )
            )
            event_ts = (
                pub[InputColumn.event_timestamp][i]
                + partner[InputColumn.event_timestamp][i]
            )
            # Validate that test_flag is same as expected
            assert pub[InputColumn.test_flag][i] == partner[InputColumn.test_flag][i]
            test_flag = pub[InputColumn.test_flag][i]
            if not is_converter:
                value = pub[InputColumn.value][i] + partner[InputColumn.value][i]
                value_sq = (
                    pub[InputColumn.value_squared][i]
                    + partner[InputColumn.value_squared][i]
                )

            valid_ts = opp_ts < event_ts + CONST_TS_OFFSET
            if test_flag:
                test_pop += 1
                if valid_ts:
                    test_events += 1
                    if not is_converter:
                        # pyre-fixme[61]: `value` may not be initialized here.
                        test_sales += value
                        # pyre-fixme[61]: `value_sq` may not be initialized here.
                        test_sq += value_sq
            else:
                control_pop += 1
                if valid_ts:
                    control_events += 1
                    if not is_converter:
                        # pyre-fixme[61]: `value` may not be initialized here.
                        control_sales += value
                        # pyre-fixme[61]: `value_sq` may not be initialized here.
                        control_sq += value_sq
        res = {Metric.test_population: test_pop, Metric.control_population: control_pop}

        if is_converter:
            res.update(
                {
                    Metric.test_purchasers: test_events,
                    Metric.control_purchasers: control_events,
                }
            )
        else:
            # Conversion lift uses _conversions instead of _purchasers
            res.update(
                {
                    Metric.test_conversions: test_events,
                    Metric.control_conversions: control_events,
                    # pyre-fixme[61]: `test_sales` may not be initialized here.
                    Metric.test_sales: test_sales,
                    # pyre-fixme[61]: `control_sales` may not be initialized here.
                    Metric.control_sales: control_sales,
                    # pyre-fixme[61]: `test_sq` may not be initialized here.
                    Metric.test_sales_squared: test_sq,
                    # pyre-fixme[61]: `control_sq` may not be initialized here.
                    Metric.control_sales_squared: control_sq,
                }
            )
        return res
