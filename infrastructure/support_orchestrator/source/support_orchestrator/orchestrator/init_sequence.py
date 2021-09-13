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
import sys
from pathlib import Path
from traceback import format_exception
from typing import Any, Dict

from support_orchestrator.const import (
    AppConfig,
    HBaseTables,
    OrchestratorConfYaml,
    OrchestratorEnvs,
)
from support_orchestrator.orchestrator.deployments import DeploymentManager
from support_orchestrator.utils.find import (
    get_active_flink_client,
    get_active_hbase_client,
    get_flink_job_manager,
    get_spark_job_manager,
)
from support_orchestrator.utils.load import load_flink_jobs

LOGGER = logging.getLogger(__name__)


def init(app_config: Dict[str, Any]) -> None:
    init_const(app_config)
    init_logging(app_config)
    init_tables(app_config)
    init_flink_jobs(app_config)
    init_deployments(app_config)


def init_const(app_config: Dict[str, Any]) -> None:
    app_config[AppConfig.SPARK_SUBMIT_BIN_KEY] = (
        Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.SPARK_HOME]) / "bin" / "spark-submit"
    )


def init_flink_jobs(app_config: Dict[str, Any]) -> None:
    flink_client = get_active_flink_client(app_config)
    hbase_client = get_active_hbase_client(app_config)
    get_spark_job_manager(app_config)
    flink_job_manager = get_flink_job_manager(app_config)
    load_flink_jobs(flink_client, hbase_client, flink_job_manager)
    LOGGER.info("Flink Jobs:")
    LOGGER.info(f'{hbase_client.select("SELECT * FROM FLINK_JOBS")}')


def init_tables(app_config: Dict[str, Any]) -> None:
    hbase_client = get_active_hbase_client(app_config)
    if os.getenv(OrchestratorEnvs.SUPPORT_PURGE_FLINK_JOBS) == "1":
        hbase_client.execute(HBaseTables.FLINK_JOBS.drop_if_exists())
    if os.getenv(OrchestratorEnvs.SUPPORT_PURGE_DATABASE) == "1":
        hbase_client.execute(HBaseTables.DATASETS.drop_if_exists())
        hbase_client.execute(HBaseTables.FLINK_DATAFLOWS.drop_if_exists())
        hbase_client.execute(HBaseTables.SPARK_DATAFLOWS.drop_if_exists())
        hbase_client.execute(HBaseTables.DATA_TABLES.drop_if_exists())
    hbase_client.execute(HBaseTables.FLINK_JOBS.create_if_not_exists())
    hbase_client.execute(HBaseTables.DATASETS.create_if_not_exists())
    hbase_client.execute(HBaseTables.FLINK_DATAFLOWS.create_if_not_exists())
    hbase_client.execute(HBaseTables.SPARK_DATAFLOWS.create_if_not_exists())
    hbase_client.execute(HBaseTables.DATA_TABLES.create_if_not_exists())


def init_deployments(app_config: Dict[str, Any]) -> None:
    deployment_manager = DeploymentManager(app_config)
    app_config[AppConfig.DEPLOYMENT_MANAGER_KEY] = deployment_manager
    LOGGER.info(f"{deployment_manager.deployments}")
    for deployment in deployment_manager.deployments.values():
        deployment.deploy(app_config)


def init_logging(app_config: Dict[str, Any]) -> None:
    log_path = Path(app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.LOG_DIR])
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / "support_orchestrator.log"
    log_file.touch(exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)-15s %(message)s")
    sys.excepthook = uncaught_handler


def uncaught_handler(exc_type, exc_value, tb):
    formatted = "".join(format_exception(exc_type, exc_value, tb))
    sys.stderr.write(formatted)
    LOGGER.exception(formatted)
