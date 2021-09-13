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

import secrets
from datetime import datetime
from uuid import uuid4


def generate_uuid(prefix: str = None) -> str:
    if prefix is None:
        prefix = ""
    else:
        prefix = f"{prefix}-"
    return f"{prefix}{str(uuid4())}-{secrets.token_hex(16)}"


def generate_timestamp() -> str:
    return f"{datetime.now().isoformat(sep=' ', timespec='microseconds')}000"
