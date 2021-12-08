# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

"""
PrivateComputationBaseStageFlow has a mapping from subclass name -> subclass.
This only works if the subclass is imported somewhere in the global namespace.
This logic will import all of the modules in the directory, which will guarantee
that each subclass is imported whenever PrivateComputationBaseStageFlow is imported.

Reference: https://stackoverflow.com/a/60861023/
"""

from importlib import import_module
from pathlib import Path

# grabs each python file
for f in Path(__file__).parent.glob("*.py"):
    module_name = f.stem
    # prevents circular imports
    if (not module_name.startswith("_")) and (module_name not in globals()):
        import_module(f".{module_name}", __package__)
    del f, module_name
# unload these modules
del import_module, Path
