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

from typing import Any, Dict

from support_orchestrator.clients.flink import FlinkClient
from support_orchestrator.clients.hbase import HBaseClient
from support_orchestrator.exceptions.internal import MaxIterationExceededException
from support_orchestrator.utils.find import (
    get_active_flink_client,
    get_active_hbase_client,
    get_flink_job_manager,
)
from support_orchestrator.utils.load import load_flink_jobs


class JobOrchestrator:
    MAX_ITERATION = 5

    def __init__(self, app_config: Dict[str, Any]) -> None:
        self._hbase_client = get_active_hbase_client(app_config)
        self._flink_client = get_active_flink_client(app_config)
        self._app_config = app_config

    def get_flink_job_id(self, job_name: str, iteration: int = 0) -> str:
        if iteration == self.MAX_ITERATION:
            raise MaxIterationExceededException()

        results = self._hbase_client.select(
            f"SELECT jar_id FROM FLINK_JOBS"
            f" WHERE FLINK_JOBS.job_name = '{job_name}'"
            f" AND FLINK_JOBS.jobmanager_address = '{self._flink_client.url}'"
        )
        if not results:
            self._hbase_client = get_active_hbase_client(self._app_config)
            self._flink_client = get_active_flink_client(self._app_config)
            load_flink_jobs(self._flink_client, self._hbase_client, get_flink_job_manager(self._app_config))
            return self.get_flink_job_id(job_name, iteration + 1)
        return results[0][0]

    @property
    def hbase_client(self) -> HBaseClient:
        self._hbase_client = get_active_hbase_client(self._app_config)
        return self._hbase_client

    @property
    def flink_client(self) -> FlinkClient:
        self._flink_client = get_active_flink_client(self._app_config)
        return self._flink_client

    def update_config(self, app_config: Dict[str, Any]) -> None:
        self._app_config = app_config
