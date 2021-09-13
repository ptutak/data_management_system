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

import click
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.session import SparkSession


@click.command()
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
    "--bootstrap-servers",
    type=str,
    required=True,
)
@click.option(
    "--topic",
    type=str,
    required=True,
)
def main(
    table: str,
    zookeeper_url: str,
    bootstrap_servers: str,
    topic: str,
):
    spark = SparkSession.builder.appName("HBaseToKafkaJob").getOrCreate()
    df = spark.read.format("phoenix").option("table", table).option("zkUrl", zookeeper_url).load()
    casted_df = df.select([col(column).cast("string") for column in df.columns])
    combined_df = casted_df.withColumn("value", concat_ws(",", *casted_df.columns))
    combined_df.write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("topic", topic).save()


if __name__ == "__main__":
    main()
