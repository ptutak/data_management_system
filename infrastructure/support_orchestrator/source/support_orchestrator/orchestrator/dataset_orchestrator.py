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
import os.path
from pathlib import Path
from typing import Any, Dict, List, cast

import yaml

from support_orchestrator.const import AppConfig, HBaseTables, OrchestratorConfYaml
from support_orchestrator.exceptions.response import DatasetException
from support_orchestrator.orchestrator.deployments import (
    DeploymentManager,
    DeploymentStatus,
    SparkDeployment,
)
from support_orchestrator.orchestrator.job_orchestrator import JobOrchestrator
from support_orchestrator.sql.functions import create_table, map_rows
from support_orchestrator.utils.find import get_active_hdfs_client
from support_orchestrator.utils.generate import generate_timestamp, generate_uuid
from support_orchestrator.utils.validate import validate

LOGGER = logging.getLogger(__name__)
DATASET_SCHEMA_PATH = Path(os.path.dirname(__file__)) / "schema" / "dataset_schema.yaml"


class DatasetOrchestrator:
    FLINK_FLOW_JOB_NAME = "KafkaToHdfs-0.1.5"
    HBASE_TO_KAFKA_FLOW_JOB_NAME = "hbase_to_kafka"
    HDFS_TO_HBASE_FLOW_JOB_NAME = "hdfs_to_hbase_table"
    HDFS_TO_KAFKA_FLOW_JOB_NAME = "hdfs_to_kafka"

    def __init__(self, app_config: Dict[str, Any]) -> None:
        self._app_config = app_config
        self._hdfs_namenode_master = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HDFS_NAMENODE_MASTER]
        self._schema_dir = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HDFS_SCHEMA_DIR]
        self._dataset_dir = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HDFS_DATASET_DIR]
        self._host_id = app_config[AppConfig.SUPPORT_CONFIG][OrchestratorConfYaml.HOST_ID]
        self._job_orchestrator = JobOrchestrator(app_config)
        self._deployment_manager = DeploymentManager()
        self._hdfs_client = get_active_hdfs_client(app_config)
        self._hdfs_client.makedirs(self._schema_dir)

    def datasets(self) -> List[Dict[str, Any]]:
        results = self._job_orchestrator.hbase_client.select(
            f"SELECT id, name, timestamp FROM {HBaseTables.DATASETS.name} WHERE tombstone IS NULL"
        )
        return map_rows(results, ["id", "name", "timestamp"])

    def add_dataset(self, dataset_name: str, dataset_schema: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        self._validate_schema(dataset_schema)
        dataset_uuid = generate_uuid("dataset")
        new_dataset_sql = HBaseTables.DATASETS.upsert(
            id=dataset_uuid, name=dataset_name, timestamp=generate_timestamp(), tombstone=None
        )
        self._job_orchestrator.hbase_client.execute(new_dataset_sql)
        writer = self._hdfs_client.write(f"{self._schema_dir}/{dataset_uuid}")
        if writer is None:
            raise DatasetException("No writer for HDFS", None)

        with writer as active_writer:
            active_writer.write(yaml.safe_dump(dataset_schema).encode())

        flink_flow = self._deployment_manager.prepare_deployment(
            f"FLINKFLOW-{dataset_uuid}",
            "flink",
            self.FLINK_FLOW_JOB_NAME,
            {"topic": dataset_uuid, "path": f"{self._dataset_dir}/{dataset_uuid}", "group.id": dataset_uuid},
        )
        response = flink_flow.deploy(self._app_config)
        LOGGER.info(f"Flink Deployment Response: {response}")
        if response.status == DeploymentStatus.SUCCESS:
            LOGGER.info("Deployment Success")
            add_flink_dataflow_query = HBaseTables.FLINK_DATAFLOWS.upsert(
                id=generate_uuid("flink"),
                flink_run_id=response.content["jobid"],
                dataset_id=dataset_uuid,
                timestamp=generate_timestamp(),
            )
            self._job_orchestrator.hbase_client.execute(add_flink_dataflow_query)
            return [{"id": dataset_uuid, "name": dataset_name}]
        raise DatasetException("Flink Deployment Failed", response.content)

    def get_dataset(self, dataset_id: str) -> List[Dict[str, Any]]:
        response = {}
        results = self._job_orchestrator.hbase_client.select(
            "SELECT id, name, timestamp "
            f"FROM {HBaseTables.DATASETS.name} WHERE id = '{dataset_id}' AND tombstone IS NULL"
        )

        if not results:
            return []

        response[HBaseTables.DATASETS.name] = map_rows(results, ["id", "name", "timestamp"])
        dataflows_results = self._job_orchestrator.hbase_client.select(
            "SELECT id, flink_run_id, dataset_id, timestamp "
            f"FROM {HBaseTables.FLINK_DATAFLOWS.name} WHERE dataset_id = '{dataset_id}'"
        )
        response[HBaseTables.FLINK_DATAFLOWS.name] = map_rows(
            dataflows_results, ["id", "flink_run_id", "dataset_id", "timestamp"]
        )

        spark_dataflows = self._job_orchestrator.hbase_client.select(
            f"SELECT id, pid, host_id, dataset_id, data_table_id, timestamp FROM {HBaseTables.SPARK_DATAFLOWS.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL"
        )
        response[HBaseTables.SPARK_DATAFLOWS.name] = map_rows(
            spark_dataflows, ["id", "pid", "host_id", "dataset_id", "data_table_id", "timestamp"]
        )

        data_tables = self._job_orchestrator.hbase_client.select(
            f"SELECT id, name, dataset_id, timestamp FROM {HBaseTables.DATA_TABLES.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL"
        )
        response[HBaseTables.DATA_TABLES.name] = map_rows(data_tables, ["id", "name", "dataset_id", "timestamp"])
        return [response]

    def get_dataset_table(self, dataset_id: str, table_id_or_name: str) -> List[Dict[str, Any]]:
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, dataset_id, name, timestamp FROM {HBaseTables.DATA_TABLES.name} "
            f"WHERE dataset_id = '{dataset_id}' AND (id = '{table_id_or_name}' OR name = '{table_id_or_name}') "
            "AND tombstone IS NULL"
        )
        return map_rows(response, ["id", "dataset_id", "name", "timestamp"])

    def add_dataset_table(
        self,
        dataset_id: str,
        table_name: str,
        table_schema: List[Dict[str, str]],
        select_expr: List[str],
        continuous: bool = True,
    ) -> List[Dict[str, Any]]:
        self._validate_schema(table_schema)
        if not self.get_dataset(dataset_id):
            raise DatasetException(f"Dataset {dataset_id} not found.", [{"errors": f"Dataset {dataset_id} not found"}])
        table_id = f"table_{generate_uuid().replace('-', '_')}"
        self._ensure_table(dataset_id, table_id, table_name, table_schema)

        spark_deployment_type = "spark"
        if continuous:
            spark_deployment_type = "spark_continuous"

        spark_flow_uuid = generate_uuid("spark")
        spark_flow: SparkDeployment = cast(
            SparkDeployment,
            self._deployment_manager.prepare_deployment(
                spark_flow_uuid,
                spark_deployment_type,
                self.HDFS_TO_HBASE_FLOW_JOB_NAME,
                {
                    "path": f"{self._dataset_dir}/{dataset_id}",
                    "schema_path": f"{self._schema_dir}/{dataset_id}",
                    "select_expr": select_expr,
                    "table": table_id,
                },
            ),
        )

        response = spark_flow.with_environ(SPARK_FLOW_UUID=spark_flow_uuid).deploy(self._app_config)
        LOGGER.info(f"Spark Deployment Response: {response}")
        if response.status == DeploymentStatus.SUCCESS:
            pid = response.content["PID"]
            self._job_orchestrator.hbase_client.execute(
                HBaseTables.SPARK_DATAFLOWS.upsert(
                    id=spark_flow_uuid,
                    pid=pid,
                    host_id=self._host_id,
                    dataset_id=dataset_id,
                    data_table_id=table_id,
                    timestamp=generate_timestamp(),
                    tombstone=None,
                )
            )
            return [
                {
                    "dataset_id": dataset_id,
                    "data_table_id": table_id,
                }
            ]
        return [{"errors": response.content}]

    def dataset_tables(self, dataset_id: str) -> List[Dict[str, Any]]:
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, dataset_id, name, timestamp FROM {HBaseTables.DATA_TABLES.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL"
        )
        return map_rows(response, ["id", "dataset_id", "name", "timestamp"])

    def dataset_outputs(self, dataset_id: str) -> List[Dict[str, Any]]:
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, dataset_id, timestamp FROM {HBaseTables.SPARK_DATAFLOWS.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL AND data_table_id IS NULL"
        )
        return map_rows(response, ["id", "dataset_id", "timestamp"])

    def add_dataset_output(self, dataset_id: str, select_expr: List[str]) -> List[Dict[str, Any]]:
        if not self.get_dataset(dataset_id):
            raise DatasetException(f"Dataset {dataset_id} not found.", [{"errors": f"Dataset {dataset_id} not found"}])
        spark_flow_uuid = generate_uuid("spark")
        deployment: SparkDeployment = cast(
            SparkDeployment,
            self._deployment_manager.prepare_deployment(
                f"DATASETFLOW-{spark_flow_uuid}",
                "spark",
                self.HDFS_TO_KAFKA_FLOW_JOB_NAME,
                {
                    "topic": spark_flow_uuid,
                    "path": f"{self._dataset_dir}/{dataset_id}",
                    "schema_path": f"{self._schema_dir}/{dataset_id}",
                    "select_expr": select_expr,
                },
            ),
        )
        result = deployment.with_environ(SPARK_FLOW_UUID=spark_flow_uuid).deploy(self._app_config)
        if result.status == DeploymentStatus.ERROR:
            return [{"errors": result.content}]
        pid = result.content["PID"]
        self._job_orchestrator.hbase_client.execute(
            HBaseTables.SPARK_DATAFLOWS.upsert(
                id=spark_flow_uuid,
                pid=pid,
                host_id=self._host_id,
                dataset_id=dataset_id,
                data_table_id=None,
                timestamp=generate_timestamp(),
                tombstone=None,
            )
        )
        return [{"dataset_id": dataset_id, "kafka_topic": spark_flow_uuid}]

    def get_dataset_output(self, dataset_id: str, dataset_output_id: str) -> List[Dict[str, Any]]:
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, pid, host_id, dataset_id, timestamp FROM {HBaseTables.SPARK_DATAFLOWS.name} "
            f"WHERE dataset_id = '{dataset_id}' AND id = {dataset_output_id} "
            "AND tombstone IS NULL AND data_table_id IS NULL"
        )
        return map_rows(response, ["id", "pid", "host_id", "dataset_id", "timestamp"])

    def dataset_table_outputs(self, dataset_id: str, data_table_id_or_name: str) -> List[Dict[str, Any]]:
        table_id = self._get_table_id(dataset_id, data_table_id_or_name)
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, dataset_id, table_id, timestamp FROM {HBaseTables.SPARK_DATAFLOWS.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL AND data_table_id = {table_id}"
        )
        return map_rows(response, ["id", "dataset_id", "table_id", "timestamp"])

    def get_dataset_table_output(
        self, dataset_id: str, data_table_id_or_name: str, output_id: str
    ) -> List[Dict[str, Any]]:
        table_id = self._get_table_id(dataset_id, data_table_id_or_name)
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, pid, host_id, dataset_id, table_id, timestamp FROM {HBaseTables.SPARK_DATAFLOWS.name} "
            f"WHERE dataset_id = '{dataset_id}' AND tombstone IS NULL AND data_table_id = {table_id} "
            f"AND id = {output_id}"
        )
        return map_rows(response, ["id", "pid", "host_id", "dataset_id", "table_id", "timestamp"])

    def add_dataset_table_output(self, dataset_id: str, data_table_id_or_name: str) -> List[Dict[str, Any]]:
        if not self.get_dataset(dataset_id):
            raise DatasetException(f"Dataset {dataset_id} not found.", [{"errors": f"Dataset {dataset_id} not found"}])
        table_id = self._get_table_id(dataset_id, data_table_id_or_name)
        spark_flow_uuid = generate_uuid("spark")
        deployment: SparkDeployment = cast(
            SparkDeployment,
            self._deployment_manager.prepare_deployment(
                f"HBASEFLOW-{spark_flow_uuid}",
                "spark",
                self.HBASE_TO_KAFKA_FLOW_JOB_NAME,
                {
                    "table": table_id,
                    "topic": spark_flow_uuid,
                },
            ),
        )

        result = deployment.with_environ(SPARK_FLOW_UUID=spark_flow_uuid).deploy(self._app_config)
        if result.status == DeploymentStatus.SUCCESS:
            pid = result.content["PID"]
            self._job_orchestrator.hbase_client.execute(
                HBaseTables.SPARK_DATAFLOWS.upsert(
                    id=spark_flow_uuid,
                    pid=pid,
                    host_id=self._host_id,
                    dataset_id=dataset_id,
                    data_table_id=table_id,
                    timestamp=generate_timestamp(),
                    tombstone=None,
                )
            )
            return [
                {
                    "dataset_id": dataset_id,
                    "data_table_id": table_id,
                    "kafka_topic": spark_flow_uuid,
                }
            ]
        return [{"errors": result.content}]

    def _get_table_id(self, dataset_id: str, data_table_id_or_name: str) -> str:
        response = self._job_orchestrator.hbase_client.select(
            f"SELECT id, dataset_id, name, timestamp FROM {HBaseTables.DATA_TABLES.name} "
            f"WHERE dataset_id = '{dataset_id}' AND (name = '{data_table_id_or_name}' "
            f"OR id = '{data_table_id_or_name}') AND tombstone IS NULL"
        )
        if not response:
            raise DatasetException(
                f"No such table: {dataset_id} {data_table_id_or_name}",
                [{"errors": f"No such table {data_table_id_or_name} for {dataset_id}"}],
            )
        if len(response) == 1:
            return response[0][0]
        else:
            for table_data in response:
                if table_data[0] == data_table_id_or_name:
                    return data_table_id_or_name
        raise DatasetException(
            f"Unambiguous table name: {data_table_id_or_name}",
            [{"errors": f"Unambiguous table name: {data_table_id_or_name}"}],
        )

    def _validate_schema(self, schema: List[Dict[str, str]], validate_schema_path: Path = DATASET_SCHEMA_PATH) -> None:
        result = validate({"schema": schema}, validate_schema_path)

        if result is not None:
            LOGGER.info(f"Error on schema validation {result}.")
            raise DatasetException("Error on schema validation", result)

    def _ensure_table(
        self, dataset_id: str, table_id: str, table_name: str, table_schema: List[Dict[str, str]]
    ) -> None:
        table = create_table(table_id, table_schema)
        self._job_orchestrator.hbase_client.execute(
            HBaseTables.DATA_TABLES.upsert(
                id=table_id,
                name=table_name,
                dataset_id=dataset_id,
                timestamp=generate_timestamp(),
                tombstone=None,
                on_duplicate_key="IGNORE",
            )
        )
        self._job_orchestrator.hbase_client.execute(table.create_if_not_exists())

        writer = self._hdfs_client.write(f"{self._schema_dir}/{table_id}")
        if writer is None:
            raise DatasetException("No writer for HDFS", None)

        with writer as active_writer:
            active_writer.write(yaml.safe_dump(table_schema).encode())
