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

from support_orchestrator.sql.schema import Column, Table


class OrchestratorConfYaml:
    FLINK_JOBMANAGER_ADDRESSES = "flink.jobmanager.addresses"
    FLINK_JOBS_DIR = "flink.jobs.dir"

    HBASE_MASTER_ADDRESSES = "hbase.master.addresses"
    HBASE_QUERYSERVER_ADDRESSES = "hbase.queryserver.addresses"

    SPARK_HOME = "spark.home"
    SPARK_JOBS_DIR = "spark.jobs.dir"

    DEPLOYMENTS_JOBS_DIR = "deployments.jobs.dir"

    LOG_DIR = "log.dir"
    HDFS_NAMENODE_MASTER = "hdfs.namenode.master"
    HDFS_RPC_PORT = "hdfs.rpc.port"
    HDFS_HTTP_PORT = "hdfs.http.port"
    HDFS_SCHEMA_DIR = "hdfs.schema.dir"
    HDFS_DATASET_DIR = "hdfs.dataset.dir"

    HOST_ID = "host.id"


class AppConfig:
    SUPPORT_CONFIG = "SUPPORT_CONFIG"
    JOBS_CONF = "JOBS_CONF"
    FLINK_ACTIVE_CLIENT_KEY = "flink.active.client"
    HBASE_ACTIVE_CLIENT_KEY = "hbase.active.client"
    FLINK_JOB_MANAGER_KEY = "flink.job.manager"
    SPARK_JOB_MANAGER_KEY = "spark.job.manager"
    JOB_ORCHESTRATOR_KEY = "job.orchestrator"
    DEPLOYMENT_MANAGER_KEY = "deployment.manager"
    SPARK_SUBMIT_BIN_KEY = "spark.submit.bin"


class OrchestratorEnvs:
    SUPPORT_PURGE_DATABASE = "SUPPORT_PURGE_DATABASE"
    SUPPORT_PURGE_FLINK_JOBS = "SUPPORT_PURGE_FLINK_JOBS"
    SUPPORT_HOME = "SUPPORT_HOME"


class HBaseTypes:
    VARCHAR = "VARCHAR"


class HBaseTables:
    FLINK_JOBS = Table(
        "FLINK_JOBS",
        Column("job_name", "VARCHAR", primary_key=True, not_null=True),
        Column("jobmanager_address", "VARCHAR", primary_key=True, not_null=True),
        Column("jar_id", "VARCHAR"),
    )

    DATASETS = Table(
        "DATASETS",
        Column("id", "VARCHAR", primary_key=True, not_null=True),
        Column("name", "VARCHAR"),
        Column("timestamp", "TIMESTAMP"),
        Column("tombstone", "TIMESTAMP"),
    )

    FLINK_DATAFLOWS = Table(
        "FLINK_DATAFLOWS",
        Column("id", "VARCHAR", primary_key=True, not_null=True),
        Column("flink_run_id", "VARCHAR"),
        Column("dataset_id", "VARCHAR"),
        Column("timestamp", "TIMESTAMP"),
    )

    SPARK_DATAFLOWS = Table(
        "SPARK_DATAFLOWS",
        Column("id", "VARCHAR", primary_key=True, not_null=True),
        Column("pid", "INTEGER"),
        Column("host_id", "VARCHAR"),
        Column("dataset_id", "VARCHAR"),
        Column("data_table_id", "VARCHAR"),
        Column("timestamp", "TIMESTAMP"),
        Column("tombstone", "TIMESTAMP"),
    )

    DATA_TABLES = Table(
        "DATA_TABLES",
        Column("id", "VARCHAR", primary_key=True, not_null=True),
        Column("dataset_id", "VARCHAR"),
        Column("name", "VARCHAR"),
        Column("timestamp", "TIMESTAMP"),
        Column("tombstone", "TIMESTAMP"),
    )
