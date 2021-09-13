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

from typing import Any, Dict, List

import click
import pyspark.sql.types as spark_types
from click.types import Path
from hdfs import InsecureClient
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType
from yaml import safe_load


@click.command()
@click.option(
    "--hdfs-ip",
    type=str,
    required=True,
)
@click.option(
    "--hdfs-http-port",
    type=int,
    required=True,
)
@click.option(
    "--hdfs-rpc-port",
    type=int,
    required=True,
)
@click.option(
    "--path",
    type=Path(),
    required=True,
)
@click.option(
    "--bootstrap-servers",
    type=str,
    required=True,
)
@click.option(
    "--topic",
    type=str,
    required=True,
)
@click.option(
    "--select-expr",
    multiple=True,
    default=[],
)
@click.option(
    "--schema-path",
    type=str,
)
@click.option(
    "--continuous-mode",
    is_flag=True,
)
def main(
    hdfs_ip: str,
    hdfs_http_port: int,
    hdfs_rpc_port: int,
    path: str,
    topic: str,
    bootstrap_servers: str,
    schema_path: str,
    select_expr: List[str],
    continuous_mode: bool,
):
    spark = SparkSession.builder.appName("HdfsToKafkaJob").getOrCreate()
    schema = load_schema(f"{hdfs_ip}:{hdfs_http_port}", schema_path)
    df = (
        spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", ",")
        .option("recursiveFileLookup", "true")
        .schema(schema)
        .load(f"hdfs://{hdfs_ip}:{hdfs_rpc_port}{path}/*")
    )
    if select_expr:
        df = df.selectExpr(*select_expr)
    casted_df = df.select([col(column).cast("string") for column in df.columns])
    combined_df = casted_df.withColumn("value", concat_ws(",", *casted_df.columns))
    if continuous_mode:
        combined_df.writeStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option(
            "topic", topic
        ).start()
    else:
        combined_df.write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option(
            "topic", topic
        ).save()


def load_schema(hdfs_ip_port: str, schema_path: str) -> StructType:
    schema: List[Dict[str, Any]] = load_yaml(hdfs_ip_port, schema_path)
    return StructType(
        [
            StructField(
                field["column_name"],
                getattr(spark_types, field["spark_type"])(),
                field.get("nullable", False),
            )
            for field in schema
        ]
    )


def load_yaml(hdfs_ip_port: str, schema_path: str) -> List[Dict[str, Any]]:
    client = InsecureClient(f"http://{hdfs_ip_port}", user="spark")
    with client.read(schema_path) as reader:
        content: str = reader.read().decode()
        return safe_load(content)


if __name__ == "__main__":
    main()
