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
    "http://192.168.2.201:17777/api/v1/datasets/dataset-e7a36f0c-dcd4-4a31-a6ba-46c428bbcb24-1017c3398a588387097c8abde65c5446/tables"
    "/table_d426e06a_4f82_4e3f_b382_587046615fef_18eb265933768c6760a3bbc083b185f5/output",
)

print(response)
print(response.json())
