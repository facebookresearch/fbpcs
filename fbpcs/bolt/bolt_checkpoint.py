# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict

from typing import Any, Dict, Optional

from fbpcs.bolt.bolt_job import BoltCreateInstanceArgs, BoltJob, BoltPlayerArgs
from fbpcs.common.service.write_checkpoint import write_checkpoint


class bolt_checkpoint(write_checkpoint):
    _DEFAULT_REGISTRY_KEY = "bolt_checkpoint_key"
    _PARAMS_CONTAINING_INSTANCE_ID = [
        "instance_id",
        "publisher_id",
        "partner_id",
        "job",
        "instance_args",
    ]
    _DEFAULT_COMPONENT_NAME = "Bolt"

    @classmethod
    def _param_to_instance_id(
        cls, instance_id_param: str, kwargs: Dict[str, Any]
    ) -> Optional[str]:
        instance_id_obj = kwargs.get(instance_id_param)
        if not instance_id_obj:
            return instance_id_obj
        elif isinstance(instance_id_obj, str):
            return instance_id_obj
        elif isinstance(instance_id_obj, BoltCreateInstanceArgs):
            return instance_id_obj.instance_id
        elif isinstance(instance_id_obj, BoltPlayerArgs):
            return instance_id_obj.create_instance_args.instance_id
        elif isinstance(instance_id_obj, BoltJob):
            return instance_id_obj.publisher_bolt_args.create_instance_args.instance_id
        else:
            return super()._param_to_instance_id(
                instance_id_param=instance_id_param, kwargs=kwargs
            )
