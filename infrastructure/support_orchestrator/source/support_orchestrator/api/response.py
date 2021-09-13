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

from enum import Enum
from typing import Any, Dict


class ResponseState(Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class ApiResponse:
    def __init__(self, status: ResponseState, response: Any) -> None:
        self._status = status
        self._response = response

    @property
    def status(self) -> ResponseState:
        return self._status

    @property
    def response(self) -> Any:
        return self._response

    def to_jsonable(self) -> Dict:
        return {"status": self.status.value, "response": self.response}
