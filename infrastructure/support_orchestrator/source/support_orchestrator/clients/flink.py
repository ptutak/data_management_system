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
from typing import Any, Dict

import requests
from requests.models import Response

from support_orchestrator.exceptions.response import FlinkHTTPException
from support_orchestrator.utils.args import format_args

LOGGER = logging.getLogger(__name__)


class FlinkClient:
    CONFIG_URL = "{url}/config"
    JARS_URL = "{url}/jars"

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._url = f"http://{host}:{port}"

    @property
    def url(self) -> str:
        return self._url

    def get_jars(self) -> Dict:
        response = requests.get(self.JARS_URL.format(url=self._url))
        return self.process_response(response)

    def upload_jar(self, jar_path: Path) -> Dict:
        with open(str(jar_path), "rb") as content:
            jar_content = content.read()
        response = requests.post(
            f"{self.JARS_URL}/upload".format(url=self._url),
            files={"file": (str(jar_path), jar_content, "application/x-java-archive")},
        )
        return self.process_response(response)

    def run_jar(self, jar_id: str, *args, **kwargs) -> Dict:
        LOGGER.info(f"Run Jar: {jar_id}, {format_args(*args, **kwargs)}")
        response = requests.post(
            f"{self.JARS_URL}/{jar_id}/run".format(url=self._url),
            json={"programArgsList": format_args(*args, **kwargs)},
        )
        return self.process_response(response)

    def get_config(self) -> Dict:
        response = requests.get(self.CONFIG_URL.format(url=self._url))
        return self.process_response(response)

    def process_response(self, response: Response) -> Dict[str, Any]:
        if response.status_code == 200:
            return response.json()
        raise FlinkHTTPException(str(response.text))


if __name__ == "__main__":
    client = FlinkClient("192.168.2.121", 8081)
