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

import requests
from pprint import pformat

response = requests.get(
    "http://192.168.2.201:17777/api/v1/datasets/"
    "dataset-cdad9bd9-0a0a-4089-8603-7e049c8bab18-440d47b6447430412c65a78bff08e2a3/tables/"
    "table_a38fdbdf_a21e_4257_88a8_a8da69a16d01_184628f7ab115b2ca4ecdf95270fc424"
)

print(response)
print(pformat(response.json()))
