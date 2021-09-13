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

import os
from pathlib import Path

import yaml

SECRET_KEY = os.urandom(16)

SUPPORT_CONF_DIR = os.getenv("SUPPORT_CONF_DIR")

if SUPPORT_CONF_DIR is None:
    raise RuntimeError("SUPPORT_CONF_DIR is not defined")

with open(Path(SUPPORT_CONF_DIR, "orchestrator-conf.yaml")) as config:
    SUPPORT_CONFIG = yaml.safe_load(config.read())

JOBS_CONF_FILE = Path(SUPPORT_CONF_DIR) / "jobs-conf.yaml"

if JOBS_CONF_FILE.is_file():
    with open(JOBS_CONF_FILE) as config:
        JOBS_CONF = yaml.safe_load(config.read())
else:
    JOBS_CONF = None
