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
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from hdfs import InsecureClient

from support_orchestrator.clients.flink import FlinkClient
from support_orchestrator.clients.hbase import HBaseClient
from support_orchestrator.clients.spark import SparkClient
from support_orchestrator.const import AppConfig, OrchestratorConfYaml, OrchestratorEnvs
from support_orchestrator.orchestrator.jobs import JobManager

LOGGER = logging.getLogger(__name__)


def find_flink_active_jobmanager(flink_jobmanager_addresses: List[str]) -> FlinkClient:
    for address in flink_jobmanager_addresses:
        host, port = address.split(":")
        client = FlinkClient(host, int(port))
        try:
            client.get_config()
            return client
        except Exception:
            pass

    raise RuntimeError("Cannot find active flink jobmanager.")


def get_active_flink_client(app_config: Dict[str, Any]) -> FlinkClient:
    active_flink_client: Optional[FlinkClient] = app_config.get(AppConfig.FLINK_ACTIVE_CLIENT_KEY, None)
    try:
        if active_flink_client is not None:
            active_flink_client.get_config()
    except Exception:
        active_flink_client = None

    if active_flink_client is None:
        active_flink_client = find_flink_active_jobmanager(
            app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.FLINK_JOBMANAGER_ADDRESSES]
        )
    app_config[AppConfig.FLINK_ACTIVE_CLIENT_KEY] = active_flink_client
    return active_flink_client


def find_hbase_active_queryserver(
    hbase_queryserver_addresses: List[str],
) -> HBaseClient:
    for address in hbase_queryserver_addresses:
        client = HBaseClient(address)
        try:
            client.use_default()
            return client
        except Exception as e:
            LOGGER.info(str(e))

    raise RuntimeError("Cannot find active hbase queryserver")


def get_active_hbase_client(app_config: Dict[str, Any]) -> HBaseClient:
    active_hbase_client: Optional[HBaseClient] = app_config.get(AppConfig.HBASE_ACTIVE_CLIENT_KEY)
    try:
        if active_hbase_client is not None:
            active_hbase_client.use_default()
    except Exception:
        active_hbase_client = None

    if active_hbase_client is None:
        active_hbase_client = find_hbase_active_queryserver(
            app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HBASE_QUERYSERVER_ADDRESSES]
        )

    app_config[AppConfig.HBASE_ACTIVE_CLIENT_KEY] = active_hbase_client
    return active_hbase_client


def get_active_spark_client(app_config: Dict[str, Any]) -> SparkClient:
    return SparkClient(
        Path(app_config[AppConfig.SPARK_SUBMIT_BIN_KEY]), Path(os.environ[OrchestratorEnvs.SUPPORT_HOME])
    )


def get_flink_job_manager(app_config: Dict[str, Any]) -> JobManager:
    jobs_path = Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.FLINK_JOBS_DIR])
    if app_config.get(AppConfig.FLINK_JOB_MANAGER_KEY) is None:
        app_config[AppConfig.FLINK_JOB_MANAGER_KEY] = JobManager(jobs_path, ".jar")
    return app_config[AppConfig.FLINK_JOB_MANAGER_KEY]


def get_spark_job_manager(app_config: Dict[str, Any]) -> JobManager:
    jobs_path = Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.SPARK_JOBS_DIR])
    if app_config.get(AppConfig.SPARK_JOB_MANAGER_KEY) is None:
        app_config[AppConfig.SPARK_JOB_MANAGER_KEY] = JobManager(jobs_path, ".py")
    return app_config[AppConfig.SPARK_JOB_MANAGER_KEY]


def get_active_hdfs_client(app_config: Dict[str, Any], user: str = "hdfs") -> InsecureClient:
    hdfs_namenode_master = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HDFS_NAMENODE_MASTER]
    hdfs_http_port = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HDFS_HTTP_PORT]
    return InsecureClient(f"http://{hdfs_namenode_master}:{hdfs_http_port}", user)


def get_support_config(app_config: Dict[str, Any]) -> Dict[str, Any]:
    return app_config[AppConfig.SUPPORT_CONFIG]


def get_log_dir(app_config: Dict[str, Any]) -> Path:
    return Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.LOG_DIR])
