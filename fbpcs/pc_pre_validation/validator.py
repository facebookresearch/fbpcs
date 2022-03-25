#!/usr/bin/env python3
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

import abc

from fbpcs.pc_pre_validation.enums import ValidationResult
from fbpcs.pc_pre_validation.validation_report import ValidationReport


class Validator(abc.ABC):
    def validate(self) -> ValidationReport:
        """A wrapper for __validator__().

        In case an unexpected exception is thrown, this method will still return a SUCCESS report
        so that a bug will not block a PC run.
        """
        try:
            return self.__validate__()
        except Exception as e:
            return ValidationReport(
                ValidationResult.SUCCESS,
                self.name,
                f"WARNING: {self.name} threw an unexpected error: {e}",
            )

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """str: the name of the validator"""
        pass

    @abc.abstractmethod
    def __validate__(self) -> ValidationReport:
        """Perform a validation for a PC run.

        When implementing this method, ensure that no exceptions will be thrown from the method.
        """
        pass
