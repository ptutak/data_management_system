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

# import phoenixdb
import pyspark.sql.types as spark_types
from click.types import Path
from hdfs import InsecureClient
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructField, StructType
from yaml import safe_load


@click.command()
@click.option(
    "--path",
    type=Path(),
    required=True,
)
@click.option(
    "--hdfs-ip",
    type=str,
    required=True,
)
@click.option(
    "--hdfs-rpc-port",
    type=int,
    required=True,
)
@click.option(
    "--hdfs-http-port",
    type=int,
    required=True,
)
@click.option(
    "--schema-path",
    type=Path(),
    required=True,
)
@click.option(
    "--table",
    type=str,
    required=True,
)
@click.option(
    "--zookeeper-url",
    type=str,
    required=True,
)
@click.option(
    "--select-expr",
    type=str,
    required=True,
    multiple=True,
)
def main(
    path: str,
    hdfs_ip: str,
    hdfs_http_port: int,
    hdfs_rpc_port: int,
    schema_path: str,
    table: str,
    zookeeper_url: str,
    select_expr: List[str],
):
    spark = SparkSession.builder.appName("HdfsToHbaseTableJob").getOrCreate()
    spark_schema: StructType = load_spark_schema(f"{hdfs_ip}:{hdfs_http_port}", schema_path)
    # phoenix_schema: str = load_phoenix_schema(f"{hdfs_ip}:{hdfs_http_port}", table, schema_path, column)
    # ensure_table(queryserver_url, phoenix_schema)
    df = (
        spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", ",")
        .option("recursiveFileLookup", "true")
        .schema(spark_schema)
        .load(f"hdfs://{hdfs_ip}:{hdfs_rpc_port}{path}/*")
    )
    selected = df.selectExpr(*select_expr)
    selected.write.format("phoenix").mode("overwrite").option("table", table).option("zkUrl", zookeeper_url).save()


def load_spark_schema(hdfs_ip_port: str, schema_path: str) -> StructType:
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


# def load_phoenix_schema(hdfs_ip_port: str, table_name: str, schema_path: str, column: List[str]) -> str:
#     schema: List[Dict[str, Any]] = load_yaml(hdfs_ip_port, schema_path)
#     schema = [
#         column_schema
#         for column_schema in schema
#         if column_schema["name"] in column or column_schema.get("primary_key", False)
#     ]
#     return create_table(table_name, schema, True)


# def create_table(table_name: str, schema: List[Dict[str, str]], if_not_exists: bool = True) -> str:
#     columns = []
#     for column_schema in schema:
#         column_name = column_schema["name"]
#         column_type = column_schema["hbase_type"]
#         nullable = " NOT NULL" if not column_schema.get("nullable", True) else ""
#         primary_key = " PRIMARY KEY" if column_schema.get("primary_key", False) else ""
#         columns.append(f"{column_name} {column_type}{nullable}{primary_key}")
#     table_schema_str = ",\n".join(columns)
#     if if_not_exists:
#         if_not_exists_str = " IF NOT EXISTS"
#     else:
#         if_not_exists_str = ""
#     return f"CREATE TABLE{if_not_exists_str} {table_name} ({table_schema_str})"


# def ensure_table(queryserver_url: str, phoenix_schema: str):
#     conn = phoenixdb.connect(queryserver_url, autocommit=True)
#     cursor = conn.cursor()
#     cursor.execute(phoenix_schema)


if __name__ == "__main__":
    main()
