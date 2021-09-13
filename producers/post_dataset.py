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

response = requests.post(
    "http://192.168.2.201:17777/api/v1/datasets", json={
        "name": "some_dataset",
        "schema": [
            {
                "column_name": "timestamp",
                "column_type": "biginteger",
                "nullable": False,
                "primary_key": True,
            },
            {
                "column_name": "my_column",
                "column_type": "string",
                "nullable": True,
            },
            {
                "column_name": "name",
                "column_type": "string",
                "nullable": True,
            }
        ],
    }
)


print(response)
print(response.json())
