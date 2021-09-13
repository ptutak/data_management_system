# Copyright 2021 Piotr Tutak

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from cerberus import Validator

LOGGER = logging.getLogger(__name__)


def validate(content: Dict[str, Any], schema_path: Path) -> Optional[str]:
    with open(schema_path) as file:
        schema_content = yaml.safe_load(file.read())
    LOGGER.info(f"{schema_content}")
    LOGGER.info(f"{content}")
    validator = Validator(schema_content)
    if not validator.validate(content):
        return str(validator.errors)
    return None
